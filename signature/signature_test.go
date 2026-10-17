package signature_test

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marcus-girolneto/hookrelay/signature"
)

// Test vector published in the Standard Webhooks reference implementations.
const (
	vecSecret = "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"
	vecID     = "msg_p5jXN8AQM9LWM0D4loKWxJek"
	vecTS     = 1614265330
	vecBody   = `{"test": 2432232314}`
	vecSig    = "v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE="
)

func fixedClock(unix int64) signature.Option {
	return signature.WithClock(func() time.Time { return time.Unix(unix, 0) })
}

func TestSignMatchesSpecVector(t *testing.T) {
	t.Parallel()
	s, err := signature.NewSigner(vecSecret)
	if err != nil {
		t.Fatal(err)
	}
	got := s.Sign(vecID, time.Unix(vecTS, 0), []byte(vecBody))
	if got != vecSig {
		t.Fatalf("Sign() = %q, want %q", got, vecSig)
	}
}

func TestVerify(t *testing.T) {
	t.Parallel()
	other := signature.NewSecret()
	otherSigner, err := signature.NewSigner(other)
	if err != nil {
		t.Fatal(err)
	}
	foreignSig := otherSigner.Sign(vecID, time.Unix(vecTS, 0), []byte(vecBody))
	ts := strconv.Itoa(vecTS)

	tests := []struct {
		name    string
		id      string
		ts      string
		sigs    string
		body    string
		now     int64
		wantErr error
	}{
		{name: "valid", id: vecID, ts: ts, sigs: vecSig, body: vecBody, now: vecTS},
		{name: "valid within tolerance", id: vecID, ts: ts, sigs: vecSig, body: vecBody, now: vecTS + 299},
		{name: "valid among rotated signatures", id: vecID, ts: ts, sigs: foreignSig + " v2,abc " + vecSig, body: vecBody, now: vecTS},
		{name: "ignores malformed candidates", id: vecID, ts: ts, sigs: "garbage v1,!!! " + vecSig, body: vecBody, now: vecTS},
		{name: "tampered body", id: vecID, ts: ts, sigs: vecSig, body: `{"test": 2432232315}`, now: vecTS, wantErr: signature.ErrNoValidSignature},
		{name: "tampered id", id: vecID + "x", ts: ts, sigs: vecSig, body: vecBody, now: vecTS, wantErr: signature.ErrNoValidSignature},
		{name: "other secret", id: vecID, ts: ts, sigs: foreignSig, body: vecBody, now: vecTS, wantErr: signature.ErrNoValidSignature},
		{name: "unknown version only", id: vecID, ts: ts, sigs: strings.Replace(vecSig, "v1,", "v1a,", 1), body: vecBody, now: vecTS, wantErr: signature.ErrNoValidSignature},
		{name: "too old", id: vecID, ts: ts, sigs: vecSig, body: vecBody, now: vecTS + 301, wantErr: signature.ErrTimestampTooOld},
		{name: "too new", id: vecID, ts: ts, sigs: vecSig, body: vecBody, now: vecTS - 301, wantErr: signature.ErrTimestampTooNew},
		{name: "non-numeric timestamp", id: vecID, ts: "yesterday", sigs: vecSig, body: vecBody, now: vecTS, wantErr: signature.ErrInvalidTimestamp},
		{name: "negative timestamp", id: vecID, ts: "-5", sigs: vecSig, body: vecBody, now: vecTS, wantErr: signature.ErrInvalidTimestamp},
		{name: "missing id", ts: ts, sigs: vecSig, body: vecBody, now: vecTS, wantErr: signature.ErrMissingHeader},
		{name: "missing timestamp", id: vecID, sigs: vecSig, body: vecBody, now: vecTS, wantErr: signature.ErrMissingHeader},
		{name: "missing signature", id: vecID, ts: ts, body: vecBody, now: vecTS, wantErr: signature.ErrMissingHeader},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			v, err := signature.NewVerifier(vecSecret, fixedClock(tc.now))
			if err != nil {
				t.Fatal(err)
			}
			h := http.Header{}
			h.Set(signature.HeaderID, tc.id)
			h.Set(signature.HeaderTimestamp, tc.ts)
			h.Set(signature.HeaderSignature, tc.sigs)
			err = v.Verify(h, []byte(tc.body))
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Verify() error = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestWithTolerance(t *testing.T) {
	t.Parallel()
	v, err := signature.NewVerifier(vecSecret, fixedClock(vecTS+10), signature.WithTolerance(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	err = v.VerifyValues(vecID, strconv.Itoa(vecTS), vecSig, []byte(vecBody))
	if !errors.Is(err, signature.ErrTimestampTooOld) {
		t.Fatalf("got %v, want ErrTimestampTooOld", err)
	}
}

func TestDecodeSecret(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		secret  string
		wantLen int
		wantErr bool
	}{
		{name: "prefixed", secret: vecSecret, wantLen: 24},
		{name: "unprefixed", secret: strings.TrimPrefix(vecSecret, signature.SecretPrefix), wantLen: 24},
		{name: "generated", secret: signature.NewSecret(), wantLen: 32},
		{name: "not base64", secret: "whsec_***", wantErr: true},
		{name: "too short", secret: "whsec_c2hvcnQ=", wantErr: true},
		{name: "empty", secret: "", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			key, err := signature.DecodeSecret(tc.secret)
			if tc.wantErr {
				if !errors.Is(err, signature.ErrInvalidSecret) {
					t.Fatalf("err = %v, want ErrInvalidSecret", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(key) != tc.wantLen {
				t.Fatalf("len(key) = %d, want %d", len(key), tc.wantLen)
			}
		})
	}
	if _, err := signature.NewSigner("bad"); err == nil {
		t.Fatal("NewSigner accepted an invalid secret")
	}
	if _, err := signature.NewVerifier("bad"); err == nil {
		t.Fatal("NewVerifier accepted an invalid secret")
	}
}

func TestSetHeadersRoundTrip(t *testing.T) {
	t.Parallel()
	secret := signature.NewSecret()
	s, _ := signature.NewSigner(secret)
	v, _ := signature.NewVerifier(secret)
	body := []byte(`{"type":"invoice.paid"}`)
	h := http.Header{}
	s.SetHeaders(h, "msg_1", time.Now(), body)
	if err := v.Verify(h, body); err != nil {
		t.Fatalf("round trip failed: %v", err)
	}
}

// FuzzRoundTrip checks the core property: whatever the id and body, a
// signature produced by Signer verifies, and any single-byte change to the
// body does not.
func FuzzRoundTrip(f *testing.F) {
	f.Add("msg_1", []byte(`{"a":1}`), int64(1_700_000_000), 0)
	f.Add("", []byte{}, int64(1), 3)
	f.Add("msg with spaces", []byte{0, 1, 2, 255}, int64(1<<40), 1)
	secret := signature.NewSecret()
	s, _ := signature.NewSigner(secret)

	f.Fuzz(func(t *testing.T, id string, body []byte, unix int64, flip int) {
		if id == "" || unix <= 0 {
			t.Skip()
		}
		v, _ := signature.NewVerifier(secret, fixedClock(unix))
		ts := time.Unix(unix, 0)
		sig := s.Sign(id, ts, body)
		tsStr := strconv.FormatInt(unix, 10)
		if err := v.VerifyValues(id, tsStr, sig, body); err != nil {
			t.Fatalf("valid signature rejected: %v", err)
		}
		if len(body) == 0 {
			return
		}
		mutated := append([]byte(nil), body...)
		i := flip % len(mutated)
		if i < 0 {
			i = -i
		}
		mutated[i] ^= 0x01
		if err := v.VerifyValues(id, tsStr, sig, mutated); !errors.Is(err, signature.ErrNoValidSignature) {
			t.Fatalf("mutated body accepted: err=%v", err)
		}
	})
}

// FuzzVerifyNoPanic feeds arbitrary header values to the verifier; it must
// never panic and must never accept a signature it did not produce.
func FuzzVerifyNoPanic(f *testing.F) {
	f.Add(vecID, strconv.Itoa(vecTS), vecSig, []byte(vecBody))
	f.Add("x", "1", "v1,", []byte{})
	f.Add("x", "99999999999999999999", "v1,AAAA v1 , ,,", []byte("{}"))
	v, _ := signature.NewVerifier(vecSecret, fixedClock(vecTS))

	f.Fuzz(func(t *testing.T, id, ts, sigs string, body []byte) {
		err := v.VerifyValues(id, ts, sigs, body)
		// ParseInt tolerates forms such as "+1614265330", which sign the
		// same canonical timestamp, so compare the parsed value.
		unix, _ := strconv.ParseInt(ts, 10, 64)
		if err == nil && (id != vecID || unix != vecTS || string(body) != vecBody) {
			// A nil error is only legitimate if the input reproduces the
			// known-good vector, possibly with extra candidates around it.
			t.Fatalf("accepted unexpected input id=%q ts=%q sigs=%q", id, ts, sigs)
		}
	})
}

func BenchmarkSign(b *testing.B) {
	s, _ := signature.NewSigner(vecSecret)
	ts := time.Unix(vecTS, 0)
	for _, size := range []int{256, 4 << 10, 64 << 10} {
		body := []byte(strings.Repeat("x", size))
		b.Run(strconv.Itoa(size)+"B", func(b *testing.B) {
			b.SetBytes(int64(size))
			b.ReportAllocs()
			for b.Loop() {
				_ = s.Sign(vecID, ts, body)
			}
		})
	}
}

func BenchmarkVerify(b *testing.B) {
	v, _ := signature.NewVerifier(vecSecret, fixedClock(vecTS))
	body := []byte(vecBody)
	ts := strconv.Itoa(vecTS)
	b.ReportAllocs()
	for b.Loop() {
		if err := v.VerifyValues(vecID, ts, vecSig, body); err != nil {
			b.Fatal(err)
		}
	}
}
