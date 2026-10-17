// Package signature signs and verifies webhook requests per the Standard
// Webhooks spec (https://www.standardwebhooks.com).
//
//	webhook-id:        unique message identifier, stable across retries
//	webhook-timestamp: Unix seconds at signing time
//	webhook-signature: space-separated "v1,<base64 HMAC-SHA256>" values
//
// The signed content is "<id>.<timestamp>.<body>".
package signature

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Header names defined by the Standard Webhooks specification.
const (
	HeaderID        = "webhook-id"
	HeaderTimestamp = "webhook-timestamp"
	HeaderSignature = "webhook-signature"
)

const (
	// SecretPrefix marks a base64-encoded signing secret.
	SecretPrefix = "whsec_"
	// DefaultTolerance is the maximum accepted clock skew.
	DefaultTolerance = 5 * time.Minute

	minSecretLen = 24
	maxSecretLen = 64
	version      = "v1"
)

// Errors may be wrapped, match with errors.Is.
var (
	ErrInvalidSecret    = errors.New("signature: invalid secret")
	ErrMissingHeader    = errors.New("signature: missing required header")
	ErrInvalidTimestamp = errors.New("signature: invalid timestamp")
	ErrTimestampTooOld  = errors.New("signature: timestamp too old")
	ErrTimestampTooNew  = errors.New("signature: timestamp too far in the future")
	ErrNoValidSignature = errors.New("signature: no matching signature")
)

// NewSecret returns a freshly generated secret in "whsec_<base64>" form.
func NewSecret() string {
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	return SecretPrefix + base64.StdEncoding.EncodeToString(key)
}

// DecodeSecret decodes a "whsec_<base64>" secret into a 24-64 byte key.
func DecodeSecret(secret string) ([]byte, error) {
	key, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(secret, SecretPrefix))
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidSecret, err)
	}
	if len(key) < minSecretLen || len(key) > maxSecretLen {
		return nil, fmt.Errorf("%w: key must be %d-%d bytes, got %d", ErrInvalidSecret, minSecretLen, maxSecretLen, len(key))
	}
	return key, nil
}

// Signer signs outgoing requests. Safe for concurrent use.
type Signer struct {
	key []byte
}

// NewSigner returns a Signer for the given secret.
func NewSigner(secret string) (*Signer, error) {
	key, err := DecodeSecret(secret)
	if err != nil {
		return nil, err
	}
	return &Signer{key: key}, nil
}

// Sign returns the webhook-signature header value for a message.
func (s *Signer) Sign(msgID string, ts time.Time, body []byte) string {
	mac := s.mac(msgID, ts.Unix(), body)
	out := make([]byte, 0, len(version)+1+base64.StdEncoding.EncodedLen(len(mac)))
	out = append(out, version...)
	out = append(out, ',')
	out = base64.StdEncoding.AppendEncode(out, mac)
	return string(out)
}

// SetHeaders sets the id, timestamp and signature headers on h.
func (s *Signer) SetHeaders(h http.Header, msgID string, ts time.Time, body []byte) {
	h.Set(HeaderID, msgID)
	h.Set(HeaderTimestamp, strconv.FormatInt(ts.Unix(), 10))
	h.Set(HeaderSignature, s.Sign(msgID, ts, body))
}

func (s *Signer) mac(msgID string, unix int64, body []byte) []byte {
	return computeMAC(s.key, msgID, unix, body)
}

func computeMAC(key []byte, msgID string, unix int64, body []byte) []byte {
	var tsBuf [20]byte
	h := hmac.New(sha256.New, key)
	h.Write([]byte(msgID))
	h.Write([]byte{'.'})
	h.Write(strconv.AppendInt(tsBuf[:0], unix, 10))
	h.Write([]byte{'.'})
	h.Write(body)
	return h.Sum(nil)
}

// Verifier checks incoming requests. Safe for concurrent use.
type Verifier struct {
	key       []byte
	tolerance time.Duration
	now       func() time.Time
}

// Option configures a Verifier.
type Option func(*Verifier)

// WithTolerance overrides DefaultTolerance.
func WithTolerance(d time.Duration) Option {
	return func(v *Verifier) { v.tolerance = d }
}

// WithClock overrides the clock used to evaluate timestamp freshness.
func WithClock(now func() time.Time) Option {
	return func(v *Verifier) { v.now = now }
}

// NewVerifier returns a Verifier for the given secret.
func NewVerifier(secret string, opts ...Option) (*Verifier, error) {
	key, err := DecodeSecret(secret)
	if err != nil {
		return nil, err
	}
	v := &Verifier{key: key, tolerance: DefaultTolerance, now: time.Now}
	for _, opt := range opts {
		opt(v)
	}
	return v, nil
}

// Verify authenticates body using the Standard Webhooks headers in h.
func (v *Verifier) Verify(h http.Header, body []byte) error {
	return v.VerifyValues(h.Get(HeaderID), h.Get(HeaderTimestamp), h.Get(HeaderSignature), body)
}

// VerifyValues is like Verify but takes raw header values.
func (v *Verifier) VerifyValues(msgID, timestamp, signatures string, body []byte) error {
	if msgID == "" || timestamp == "" || signatures == "" {
		return ErrMissingHeader
	}
	unix, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil || unix <= 0 {
		return ErrInvalidTimestamp
	}
	now := v.now().Unix()
	tol := int64(v.tolerance / time.Second)
	switch {
	case unix < now-tol:
		return ErrTimestampTooOld
	case unix > now+tol:
		return ErrTimestampTooNew
	}

	expected := computeMAC(v.key, msgID, unix, body)
	for candidate := range strings.FieldsSeq(signatures) {
		ver, sig, ok := strings.Cut(candidate, ",")
		if !ok || ver != version {
			continue
		}
		raw, err := base64.StdEncoding.DecodeString(sig)
		if err != nil {
			continue
		}
		if hmac.Equal(raw, expected) {
			return nil
		}
	}
	return ErrNoValidSignature
}
