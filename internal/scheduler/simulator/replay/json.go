package replay

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"unicode/utf8"
)

const maxJSONEntries = 20_000

func readLocal(path string, limit int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	raw, err := readBounded(f, limit)
	return raw, errors.Join(err, f.Close())
}

func readBounded(r io.Reader, limit int64) ([]byte, error) {
	raw, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err == nil && int64(len(raw)) > limit {
		err = fmt.Errorf("input exceeds %d bytes", limit)
	}
	return raw, err
}

func strictJSON(raw []byte, dst any) error {
	if !utf8.Valid(raw) {
		return errors.New("JSON must be UTF-8")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := checkJSONValue(dec, 0); err != nil {
		return err
	}
	if _, err := dec.Token(); err != io.EOF {
		return errors.New("extra data after JSON value")
	}
	dec = json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	return dec.Decode(dst)
}

func checkJSONValue(dec *json.Decoder, depth int) error {
	if depth > 16 {
		return errors.New("JSON nesting is too deep")
	}
	token, err := dec.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	var seen map[string]bool
	if delim == '{' {
		seen = make(map[string]bool)
	}
	for count := 0; dec.More(); count++ {
		if count >= maxJSONEntries {
			return errors.New("JSON collection has too many entries")
		}
		if seen != nil {
			key, err := dec.Token()
			if err != nil {
				return err
			}
			name := key.(string)
			if seen[name] {
				return fmt.Errorf("duplicate JSON key %q", name)
			}
			seen[name] = true
		}
		if err := checkJSONValue(dec, depth+1); err != nil {
			return err
		}
	}
	_, err = dec.Token()
	return err
}
