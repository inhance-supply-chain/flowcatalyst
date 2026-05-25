// Package webhook validates incoming webhook requests from the
// FlowCatalyst platform using HMAC-SHA256 signatures.
//
// The signed message is the timestamp string concatenated with the raw
// request body (no separator). The signature is hex-encoded and lives
// in X-FlowCatalyst-Signature; the timestamp (Unix seconds) is in
// X-FlowCatalyst-Timestamp.
//
// Mirrors the Rust SDK's webhook module byte-for-byte so the same
// signing secret works against any FlowCatalyst SDK.
package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

// Header names sent by the FlowCatalyst platform.
const (
	SignatureHeader = "X-FlowCatalyst-Signature"
	TimestampHeader = "X-FlowCatalyst-Timestamp"
)

// DefaultToleranceSecs is how stale a timestamp may be before rejection.
const DefaultToleranceSecs = 300

// FutureGraceSecs caps how far into the future a timestamp may be (for
// clock skew between the platform and the consumer).
const FutureGraceSecs = 60

// Sentinel validation errors. Use errors.Is to branch.
var (
	ErrMissingSignature  = errors.New("webhook: missing signature header (" + SignatureHeader + ")")
	ErrMissingTimestamp  = errors.New("webhook: missing timestamp header (" + TimestampHeader + ")")
	ErrInvalidTimestamp  = errors.New("webhook: invalid timestamp: not an integer")
	ErrInvalidSignature  = errors.New("webhook: invalid signature")
	ErrTimestampExpired  = errors.New("webhook: timestamp expired")
	ErrTimestampInFuture = errors.New("webhook: timestamp is in the future")
	ErrMissingSecret     = errors.New("webhook: signing secret not configured")
)

// Validator validates HMAC-SHA256 webhook signatures.
type Validator struct {
	secret    []byte
	tolerance time.Duration
	now       func() time.Time
}

// Option configures a Validator.
type Option func(*Validator)

// WithTolerance sets the timestamp freshness window. Default 300s.
func WithTolerance(d time.Duration) Option {
	return func(v *Validator) { v.tolerance = d }
}

// WithClock overrides the time source. Tests use this to inject a
// fixed clock.
func WithClock(now func() time.Time) Option {
	return func(v *Validator) { v.now = now }
}

// New builds a Validator with the given signing secret.
func New(secret string, opts ...Option) *Validator {
	v := &Validator{
		secret:    []byte(secret),
		tolerance: DefaultToleranceSecs * time.Second,
		now:       time.Now,
	}
	for _, opt := range opts {
		opt(v)
	}
	return v
}

// FromEnv builds a Validator from FLOWCATALYST_SIGNING_SECRET. Returns
// ErrMissingSecret if the env var is unset or empty.
func FromEnv(opts ...Option) (*Validator, error) {
	s := os.Getenv("FLOWCATALYST_SIGNING_SECRET")
	if s == "" {
		return nil, ErrMissingSecret
	}
	return New(s, opts...), nil
}

// Validate checks the signature against the body and timestamp.
//
//   - signature: value of X-FlowCatalyst-Signature header (hex-encoded HMAC-SHA256)
//   - timestamp: value of X-FlowCatalyst-Timestamp header (Unix seconds)
//   - payload: raw request body
//
// Returns nil on success, or one of the sentinel errors. The signature
// comparison is constant-time.
func (v *Validator) Validate(signature, timestamp string, payload []byte) error {
	if signature == "" {
		return ErrMissingSignature
	}
	if timestamp == "" {
		return ErrMissingTimestamp
	}
	tsSecs, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return ErrInvalidTimestamp
	}
	if err := v.validateTimestamp(tsSecs); err != nil {
		return err
	}

	expected := v.ComputeSignature(timestamp, payload)
	if !hmac.Equal([]byte(signature), []byte(expected)) {
		return ErrInvalidSignature
	}
	return nil
}

// ComputeSignature renders the expected hex-encoded HMAC-SHA256 for a
// (timestamp, payload) pair. Exposed so consumers can sign outbound
// callbacks with the same algorithm.
func (v *Validator) ComputeSignature(timestamp string, payload []byte) string {
	mac := hmac.New(sha256.New, v.secret)
	mac.Write([]byte(timestamp))
	mac.Write(payload)
	return hex.EncodeToString(mac.Sum(nil))
}

func (v *Validator) validateTimestamp(tsSecs int64) error {
	now := v.now().UTC().Unix()
	lower := now - int64(v.tolerance.Seconds())
	if tsSecs < lower {
		return fmt.Errorf("%w (tolerance: %s)", ErrTimestampExpired, v.tolerance)
	}
	if tsSecs > now+FutureGraceSecs {
		return ErrTimestampInFuture
	}
	return nil
}
