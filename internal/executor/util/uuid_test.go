package util

import (
	"testing"

	"github.com/armadaproject/armada/pkg/armadaevents"
)

func TestStringUuidsToUuids(t *testing.T) {
	tests := []struct {
		name        string
		uuidStrings []string
		want        []armadaevents.Uuid
		wantErr     bool
	}{
		{"invalid uuid", []string{"1", "2", "3"}, []armadaevents.Uuid{}, true},
		{"valid uuid", []string{"52a3cfa6-8ce1-42b1-97cf-74f1b63f21b9"}, []armadaevents.Uuid{{5954831446549021361, 10939090601399755193}}, false},
		{
			"valid uuid2",
			[]string{"52a3cfa6-8ce1-42b1-97cf-74f1b63f21b9", "59567531-2a42-4b5b-9aba-b3d400c35b4c"},
			[]armadaevents.Uuid{
				{5954831446549021361, 10939090601399755193},
				{6437461571395537755, 11149421550636325708},
			},
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := StringUuidsToUuids(tt.uuidStrings)
			if (err != nil) != tt.wantErr {
				t.Errorf("StringUuidsToUuids() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			for i, v := range got {
				if v != tt.want[i] {
					t.Errorf("StringUuidsToUuids() = %v, want %v", v, tt.want[i])
				}
			}
		})
	}
}
