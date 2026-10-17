// Package id generates prefixed ULID-style IDs, e.g. "evt_01j9z3k5q8m2r7t4v6w8y0a2c4".
package id

import (
	"crypto/rand"
	"encoding/binary"
	"time"
)

const alphabet = "0123456789abcdefghjkmnpqrstvwxyz"

// New returns a new identifier with the given prefix.
func New(prefix string) string {
	return NewAt(prefix, time.Now())
}

// NewAt returns a new identifier whose time component is t.
func NewAt(prefix string, t time.Time) string {
	var raw [16]byte
	ms := uint64(t.UnixMilli()) //nolint:gosec // pre-1970 clocks are not a supported configuration.
	var ts [8]byte
	binary.BigEndian.PutUint64(ts[:], ms)
	copy(raw[0:6], ts[2:]) // 48 bits, fine until year 10889
	_, _ = rand.Read(raw[6:])

	out := make([]byte, 0, len(prefix)+1+26)
	out = append(out, prefix...)
	out = append(out, '_')
	return string(encode(out, raw))
}

func encode(dst []byte, b [16]byte) []byte {
	hi := binary.BigEndian.Uint64(b[0:8])
	lo := binary.BigEndian.Uint64(b[8:16])
	// 26*5 = 130 bits, so the first symbol only carries 3
	var sym [26]byte
	for i := range 26 {
		shift := uint(5 * (25 - i))
		var v uint64
		switch {
		case shift >= 64:
			v = hi >> (shift - 64)
		case shift == 0:
			v = lo
		default:
			v = lo>>shift | hi<<(64-shift)
		}
		sym[i] = alphabet[v&31]
	}
	return append(dst, sym[:]...)
}
