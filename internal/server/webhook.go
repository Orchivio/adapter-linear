package server

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"sync"
)

const maxWebhookBody = 1 << 20

// Hub fans a nudge out to all active streams.
type Hub struct {
	mu   sync.Mutex
	subs map[chan struct{}]struct{}
}

func NewHub() *Hub { return &Hub{subs: map[chan struct{}]struct{}{}} }

// Subscribe returns a size-1 channel and its unsubscribe func.
func (h *Hub) Subscribe() (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	h.mu.Lock()
	h.subs[ch] = struct{}{}
	h.mu.Unlock()
	return ch, func() {
		h.mu.Lock()
		delete(h.subs, ch)
		h.mu.Unlock()
	}
}

// Nudge wakes every subscriber without blocking.
func (h *Hub) Nudge() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.subs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// WebhookHandler serves POST /webhook. An empty secret disables it (404).
// ponytail: the webhook only nudges polling; the payload is not evaluated.
func WebhookHandler(secret string, hub *Hub) http.Handler {
	mux := http.NewServeMux()
	if secret == "" {
		return mux // every path 404s: webhooks disabled
	}
	mux.HandleFunc("POST /webhook", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxWebhookBody))
		if err != nil {
			http.Error(w, "bad body", http.StatusRequestEntityTooLarge)
			return
		}
		got, err := hex.DecodeString(r.Header.Get("Linear-Signature"))
		mac := hmac.New(sha256.New, []byte(secret))
		mac.Write(body)
		if err != nil || !hmac.Equal(got, mac.Sum(nil)) {
			http.Error(w, "invalid signature", http.StatusUnauthorized)
			return
		}
		hub.Nudge()
		w.WriteHeader(http.StatusOK)
	})
	return mux
}
