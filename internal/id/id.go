// Package id generates prefixed, lexicographically sortable identifiers such
// as "evt_01j9z3k5q8m2r7t4v6w8y0a2c4".
//
// The 26 character suffix is a Crockford base32 encoding of a 48-bit
// millisecond timestamp followed by 80 random bits, the same layout as ULID.
// Time ordering keeps B-tree inserts append-mostly and makes IDs useful for
// cursor pagination; the prefix makes them self-describing in logs.
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
	binary.BigEndian.PutUint16(raw[0:2], uint16(ms>>32))
	binary.BigEndian.PutUint32(raw[2:6], uint32(ms))
	_, _ = rand.Read(raw[6:]) // crypto/rand.Read never returns an error.

	out := make([]byte, 0, len(prefix)+1+26)
	out = append(out, prefix...)
	out = append(out, '_')
	return string(encode(out, raw))
}

// encode appends the 26-character base32 form of the 128-bit value in b.
func encode(dst []byte, b [16]byte) []byte {
	hi := binary.BigEndian.Uint64(b[0:8])
	lo := binary.BigEndian.Uint64(b[8:16])
	// 128 bits are encoded as 26 symbols of 5 bits; the first symbol carries
	// only the top 3 bits, so the value is treated as left-padded with 2 zeros.
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
