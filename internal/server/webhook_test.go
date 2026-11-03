package server

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func sign(secret, body string) string {
	m := hmac.New(sha256.New, []byte(secret))
	m.Write([]byte(body))
	return hex.EncodeToString(m.Sum(nil))
}

func post(h http.Handler, sig, body string) int {
	req := httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader(body))
	req.Header.Set("Linear-Signature", sig)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec.Code
}

func TestWebhookRejectsBadSignature(t *testing.T) {
	hub := NewHub()
	ch, _ := hub.Subscribe()
	h := WebhookHandler("s3cret", hub)
	for _, sig := range []string{"", "zz", sign("wrong", "{}")} {
		if code := post(h, sig, "{}"); code != http.StatusUnauthorized {
			t.Fatalf("sig %q: code %d", sig, code)
		}
	}
	select {
	case <-ch:
		t.Fatal("nudged on bad signature")
	default:
	}
}

func TestWebhookValidSignatureNudges(t *testing.T) {
	hub := NewHub()
	ch, _ := hub.Subscribe()
	if code := post(WebhookHandler("s3cret", hub), sign("s3cret", "{}"), "{}"); code != http.StatusOK {
		t.Fatalf("code %d", code)
	}
	select {
	case <-ch:
	default:
		t.Fatal("no nudge received")
	}
}

func TestWebhookDisabledWithoutSecret(t *testing.T) {
	if code := post(WebhookHandler("", NewHub()), "", "{}"); code != http.StatusNotFound {
		t.Fatalf("code %d", code)
	}
}

func TestWebhookRejectsOversizedBody(t *testing.T) {
	big := strings.Repeat("a", maxWebhookBody+1)
	if code := post(WebhookHandler("s", NewHub()), sign("s", big), big); code != http.StatusRequestEntityTooLarge {
		t.Fatalf("code %d", code)
	}
}
