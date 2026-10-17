package id

import (
	"slices"
	"strings"
	"testing"
	"time"
)

func TestNewFormat(t *testing.T) {
	t.Parallel()
	got := New("evt")
	if !strings.HasPrefix(got, "evt_") || len(got) != len("evt_")+26 {
		t.Fatalf("unexpected id %q", got)
	}
	for _, r := range strings.TrimPrefix(got, "evt_") {
		if !strings.ContainsRune(alphabet, r) {
			t.Fatalf("id %q contains %q outside the alphabet", got, r)
		}
	}
}

func TestEncodeKnownValues(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   [16]byte
		want string
	}{
		{name: "zero", in: [16]byte{}, want: strings.Repeat("0", 26)},
		{name: "max", in: [16]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, want: "7" + strings.Repeat("z", 25)},
		{name: "one", in: [16]byte{15: 1}, want: strings.Repeat("0", 25) + "1"},
		{name: "bit 64", in: [16]byte{7: 1}, want: "0000000000000g000000000000"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := string(encode(nil, tc.in)); got != tc.want {
				t.Fatalf("encode() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestNewAtSortsByTime(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ids := make([]string, 0, 100)
	for i := range 100 {
		ids = append(ids, NewAt("x", base.Add(time.Duration(i)*time.Millisecond)))
	}
	if !slices.IsSorted(ids) {
		t.Fatal("ids generated at increasing times are not sorted")
	}
}

func TestNewUnique(t *testing.T) {
	t.Parallel()
	seen := make(map[string]struct{}, 10000)
	for range 10000 {
		v := New("x")
		if _, dup := seen[v]; dup {
			t.Fatalf("duplicate id %q", v)
		}
		seen[v] = struct{}{}
	}
}
