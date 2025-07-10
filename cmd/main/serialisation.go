package main

import (
	"bufio"
	"encoding/binary"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/gogo/protobuf/proto"
	"io"
	k8sResource "k8s.io/apimachinery/pkg/api/resource"
)

var byteOrder = binary.LittleEndian

func writeString(w io.Writer, s string) error {
	length := uint32(len(s))
	if err := binary.Write(w, byteOrder, length); err != nil {
		return err
	}
	_, err := w.Write([]byte(s))
	return err
}

func writeProtoMessage(w io.Writer, msg proto.Message) error {
	data, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	if err := binary.Write(w, byteOrder, uint32(len(data))); err != nil {
		return err
	}
	_, err = w.Write(data)
	return err
}

func writeQuantityMap(w io.Writer, m map[string]k8sResource.Quantity) error {
	if err := binary.Write(w, byteOrder, uint32(len(m))); err != nil {
		return err
	}
	for k, v := range m {
		if err := writeString(w, k); err != nil {
			return err
		}
		if err := writeProtoMessage(w, &v); err != nil {
			return err
		}
	}
	return nil
}

func writeJob(w io.Writer, job *jobdb.Job) error {
	buf := bufio.NewWriter(w)

	if err := writeString(buf, job.Id()); err != nil {
		return err
	}
	if err := writeString(buf, job.Queue()); err != nil {
		return err
	}
	if err := writeString(buf, job.Jobset()); err != nil {
		return err
	}
	if err := binary.Write(buf, byteOrder, job.Priority()); err != nil {
		return err
	}
	if err := binary.Write(buf, byteOrder, job.RequestedPriority()); err != nil {
		return err
	}
	if err := binary.Write(buf, byteOrder, job.SubmitTime().Unix()); err != nil {
		return err
	}
	if err := writeProtoMessage(buf, internaltypes.ToSchedulerObjectsJobSchedulingInfo(job.JobSchedulingInfo())); err != nil {
		return err
	}
	flags := []bool{
		job.Validated(), job.Queued(), job.CancelRequested(),
		job.CancelByJobsetRequested(), job.Cancelled(),
		job.Failed(), job.Succeeded(),
	}
	for _, flag := range flags {
		var b byte = 0
		if flag {
			b = 1
		}
		if err := buf.WriteByte(b); err != nil {
			return err
		}
	}
	if err := binary.Write(buf, byteOrder, job.QueuedVersion()); err != nil {
		return err
	}
	if err := writeQuantityMap(buf, job.AllResourceRequirements().ToMap()); err != nil {
		return err
	}
	if err := writeQuantityMap(buf, job.KubernetesResourceRequirements().ToMap()); err != nil {
		return err
	}
	return buf.Flush()
}
