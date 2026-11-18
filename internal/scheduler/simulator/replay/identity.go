package replay

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

const (
	identityRunBytes = 64 << 20
	identityFanIn    = 16
	idHeaderBytes    = 10
)

type identity struct {
	id        string
	partition uint32
	row       uint32
}

type identityRun struct {
	path string
	size int64
}

type identitySort struct {
	ctx        context.Context
	dir        string
	budget     int64
	allocated  int64
	partitions []partitionManifest
	pending    []identity
	memory     int
	levels     [][]identityRun
}

func (s *identitySort) add(row identity) error {
	s.pending = append(s.pending, row)
	s.memory += len(row.id) + 48 // ID string storage, row and slice capacity.
	if s.memory >= identityRunBytes {
		return s.flush()
	}
	return nil
}

func (s *identitySort) duplicate(a, b identity) error {
	return fmt.Errorf("duplicate logical job %q in %q row %d and %q row %d",
		a.id, s.partitions[a.partition].Path, a.row, s.partitions[b.partition].Path, b.row)
}

func (s *identitySort) flush() error {
	if len(s.pending) == 0 {
		return nil
	}
	slices.SortFunc(s.pending, func(a, b identity) int { return strings.Compare(a.id, b.id) })
	for i := 1; i < len(s.pending); i++ {
		if s.pending[i-1].id == s.pending[i].id {
			return s.duplicate(s.pending[i-1], s.pending[i])
		}
	}
	run, err := s.write(func(write func(identity) error) error {
		for _, record := range s.pending {
			if err := write(record); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	clear(s.pending)
	s.pending, s.memory = s.pending[:0], 0
	for i := 0; ; i++ {
		if i == len(s.levels) {
			s.levels = append(s.levels, nil)
		}
		s.levels[i] = append(s.levels[i], run)
		if len(s.levels[i]) < identityFanIn {
			return nil
		}
		run, err = s.combine(s.levels[i])
		if err != nil {
			return err
		}
		s.levels[i] = nil
	}
}

func (s *identitySort) finish() error {
	if len(s.levels) == 0 {
		slices.SortFunc(s.pending, func(a, b identity) int { return strings.Compare(a.id, b.id) })
		for i := 1; i < len(s.pending); i++ {
			if s.pending[i-1].id == s.pending[i].id {
				return s.duplicate(s.pending[i-1], s.pending[i])
			}
		}
		return s.ctx.Err()
	}
	if err := s.flush(); err != nil {
		return err
	}
	var tail []identityRun
	for _, level := range s.levels {
		if len(tail)+len(level) > identityFanIn {
			run, err := s.combine(tail)
			if err != nil {
				return err
			}
			tail = []identityRun{run}
		}
		tail = append(tail, level...)
	}
	return s.merge(tail, func(identity) error { return nil })
}

func (s *identitySort) combine(runs []identityRun) (identityRun, error) {
	run, err := s.write(func(add func(identity) error) error { return s.merge(runs, add) })
	if err != nil {
		return run, err
	}
	for _, input := range runs {
		if err := os.Remove(input.path); err != nil {
			return run, err
		}
		s.allocated -= input.size
	}
	return run, nil
}

func (s *identitySort) write(emit func(func(identity) error) error) (identityRun, error) {
	file, err := os.CreateTemp(s.dir, "identities-")
	if err != nil {
		return identityRun{}, err
	}
	run := identityRun{path: file.Name()}
	buffer := bufio.NewWriter(file)
	err = emit(func(record identity) error {
		if err := s.ctx.Err(); err != nil {
			return err
		}
		length := int64(idHeaderBytes + len(record.id))
		if length > s.budget-s.allocated {
			return fmt.Errorf("identity validation needs more than %d bytes of scratch space", s.budget)
		}
		var header [idHeaderBytes]byte
		binary.LittleEndian.PutUint16(header[:2], uint16(len(record.id)))
		binary.LittleEndian.PutUint32(header[2:6], record.partition)
		binary.LittleEndian.PutUint32(header[6:10], record.row)
		if _, err := buffer.Write(header[:]); err != nil {
			return err
		}
		if _, err := buffer.WriteString(record.id); err != nil {
			return err
		}
		run.size += length
		s.allocated += length
		return nil
	})
	return run, errors.Join(err, buffer.Flush(), file.Close())
}

func (s *identitySort) merge(runs []identityRun, emit func(identity) error) (result error) {
	type input struct {
		file *os.File
		data *bufio.Reader
		head identity
	}
	inputs := make([]input, 0, len(runs))
	defer func() {
		for _, file := range inputs {
			result = errors.Join(result, file.file.Close())
		}
	}()
	for _, run := range runs {
		f, err := os.Open(run.path)
		if err != nil {
			return err
		}
		b := bufio.NewReader(f)
		inputs = append(inputs, input{file: f, data: b})
		i := len(inputs) - 1
		inputs[i].head, err = readIdentity(b)
		if err != nil {
			return err
		}
	}
	var previous identity
	for {
		if err := s.ctx.Err(); err != nil {
			return err
		}
		first := -1
		for i := range inputs {
			if id := inputs[i].head.id; id != "" && (first == -1 || id < inputs[first].head.id) {
				first = i
			}
		}
		if first == -1 {
			return nil
		}
		record := inputs[first].head
		if previous.id == record.id {
			return s.duplicate(previous, record)
		}
		if err := emit(record); err != nil {
			return err
		}
		previous = record
		next, err := readIdentity(inputs[first].data)
		if err != nil && err != io.EOF {
			return err
		}
		inputs[first].head = next
	}
}

func readIdentity(r *bufio.Reader) (identity, error) {
	var header [idHeaderBytes]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return identity{}, err
	}
	var id [maxJobIDBytes]byte
	length := binary.LittleEndian.Uint16(header[:2])
	if length == 0 || int(length) > len(id) {
		return identity{}, errors.New("invalid identity scratch record length")
	}
	if _, err := io.ReadFull(r, id[:length]); err != nil {
		return identity{}, err
	}
	return identity{string(id[:length]), binary.LittleEndian.Uint32(header[2:6]), binary.LittleEndian.Uint32(header[6:10])}, nil
}

func (w *workload) validate(ctx context.Context, scratch string, diskBytes int64, validateJob func(Job) error) (result error) {
	dir, err := os.MkdirTemp(scratch, "replay-validation-")
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, os.RemoveAll(dir)) }()
	stream := workloadStream{ctx: ctx, input: w}
	defer func() { result = errors.Join(result, stream.close()) }()
	ids := identitySort{ctx: ctx, dir: dir, budget: diskBytes, partitions: w.manifest.Partitions}
	for {
		job, err := stream.next()
		if err == io.EOF {
			return ids.finish()
		}
		if err != nil {
			return err
		}
		if err := validateJob(job); err != nil {
			return fmt.Errorf("partition %q row %d: %w", w.manifest.Partitions[stream.index].Path, stream.rowNumber, err)
		}
		if err := ids.add(identity{job.ID, uint32(stream.index), uint32(stream.rowNumber)}); err != nil {
			return fmt.Errorf("validate identities in %s: %w", filepath.Base(dir), err)
		}
	}
}
