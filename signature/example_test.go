package signature_test

import (
	"fmt"
	"io"
	"net/http"

	"github.com/marcus-girolneto/hookrelay/signature"
)

// Verify the raw body as received. Re-encoded JSON won't match.
func ExampleVerifier_Verify() {
	verifier, err := signature.NewVerifier("whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw")
	if err != nil {
		panic(err)
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
		if err != nil {
			http.Error(w, "body too large", http.StatusRequestEntityTooLarge)
			return
		}
		if err := verifier.Verify(r.Header, body); err != nil {
			http.Error(w, "invalid signature", http.StatusUnauthorized)
			return
		}
		// Deduplicate on r.Header.Get(signature.HeaderID) before side effects.
		w.WriteHeader(http.StatusNoContent)
	})
	_ = handler
	fmt.Println("ok")
	// Output: ok
}
