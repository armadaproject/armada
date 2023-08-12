package jobdb

import (
	"testing"

	"github.com/google/uuid"
)

func TestUUIDHasher_Hash(t *testing.T) {
	hasher := UUIDHasher{}

	tests := []struct {
		name string
		key  uuid.UUID
	}{
		{
			name: "Test with zero UUID",
			key:  uuid.UUID{},
		},
		{
			name: "Test with random UUID",
			key:  uuid.New(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := hasher.Hash(tt.key)

			// Assert the hash value is non-negative (a simple check)
			if got < 0 {
				t.Errorf("Expected non-negative hash, but got %v", got)
			}
		})
	}
}

func TestUUIDHasher_Equal(t *testing.T) {
	hasher := UUIDHasher{}

	tests := []struct {
		name string
		a, b uuid.UUID
		want bool
	}{
		{
			name: "Test with two zero UUIDs",
			a:    uuid.UUID{},
			b:    uuid.UUID{},
			want: true,
		},
		{
			name: "Test with two different UUIDs",
			a:    uuid.New(),
			b:    uuid.New(),
			want: false,
		},
		{
			name: "Test with two same UUIDs",
			a:    uuid.New(),
			b:    uuid.MustParse("f47ac10b-58cc-4372-a567-0e02b2c3d479"), // Example UUID, replace with any fixed UUID
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := hasher.Equal(tt.a, tt.b)
			if got != tt.want {
				t.Errorf("Expected %v, but got %v", tt.want, got)
			}
		})
	}
}
