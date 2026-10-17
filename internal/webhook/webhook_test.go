package webhook

import (
	"encoding/json"
	"testing"
	"time"
)

func TestEndpointAccepts(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		types  []string
		evType string
		want   bool
	}{
		{name: "no filter accepts all", evType: "invoice.paid", want: true},
		{name: "listed type", types: []string{"invoice.paid", "invoice.voided"}, evType: "invoice.paid", want: true},
		{name: "unlisted type", types: []string{"invoice.voided"}, evType: "invoice.paid", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := (Endpoint{EventTypes: tc.types}).Accepts(tc.evType); got != tc.want {
				t.Fatalf("Accepts(%q) = %v, want %v", tc.evType, got, tc.want)
			}
		})
	}
}

func TestValidateURL(t *testing.T) {
	t.Parallel()
	tests := []struct {
		url     string
		wantErr bool
	}{
		{url: "https://example.com/hooks"},
		{url: "http://receiver:8081/webhooks"},
		{url: "ftp://example.com", wantErr: true},
		{url: "/relative", wantErr: true},
		{url: "https://", wantErr: true},
		{url: "https://user:secret" + "@" + "example.com/hooks", wantErr: true},
		{url: "http://[::1", wantErr: true},
	}
	for _, tc := range tests {
		err := ValidateURL(tc.url)
		if (err != nil) != tc.wantErr {
			t.Errorf("ValidateURL(%q) error = %v, wantErr %v", tc.url, err, tc.wantErr)
		}
	}
}

func TestEventBody(t *testing.T) {
	t.Parallel()
	ev := Event{
		ID:        "evt_1",
		Type:      `odd"type`,
		Payload:   json.RawMessage(`{"amount":42}`),
		CreatedAt: time.Date(2026, 3, 1, 12, 0, 0, 500, time.FixedZone("x", 3600)),
	}
	got := string(ev.Body())
	want := `{"id":"evt_1","type":"odd\"type","timestamp":"2026-03-01T11:00:00.0000005Z","data":{"amount":42}}`
	if got != want {
		t.Fatalf("Body() =\n%s\nwant\n%s", got, want)
	}
	if !json.Valid(ev.Body()) {
		t.Fatal("Body() is not valid JSON")
	}
	if string(ev.Body()) != got {
		t.Fatal("Body() is not deterministic")
	}
}

func TestEndpointDisabled(t *testing.T) {
	t.Parallel()
	now := time.Now()
	if (Endpoint{}).Disabled() {
		t.Fatal("zero endpoint reported disabled")
	}
	if !(Endpoint{DisabledAt: &now}).Disabled() {
		t.Fatal("endpoint with DisabledAt not reported disabled")
	}
}
