package main

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/mudler/edgevpn/pkg/hub"
)

func TestGossipHandlerPreservesDecryptedBlockAndIdentity(t *testing.T) {
	payload := []byte(`{"Index":7,"Timestamp":"2026-10-07T13:00:00Z","Storage":{"alive":{"peer-a":{"PeerID":"peer-a","Metadata":{"zone":"lan","tags":["gpu","fast"]}}},"services":{"group_worker":{"Name":"worker","Details":{"models":["tiny-model"],"address":{"host":"node-a","port":12345}}}}},"Hash":"block-hash-7","PrevHash":"block-hash-6"}`)
	compressed := gzipBytes(t, payload)
	observerPeerID.Store("observer-test-id")

	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	oldStdout := os.Stdout
	os.Stdout = write
	t.Cleanup(func() {
		os.Stdout = oldStdout
		_ = read.Close()
		_ = write.Close()
	})

	message := hub.Message{Message: string(compressed), SenderID: "delivery-peer-id", Annotations: map[string]interface{}{"fixture": "preserved"}}
	if err := (logGossipMessage{maxDecompressed: 1024 * 1024}).handle(nil, &message, nil); err != nil {
		t.Fatalf("handler returned error: %v", err)
	}
	if err := write.Close(); err != nil {
		t.Fatal(err)
	}
	line, err := io.ReadAll(read)
	if err != nil {
		t.Fatal(err)
	}
	var event struct {
		Event         string          `json:"event"`
		ObserverID    string          `json:"observer_peer_id"`
		ReceivedFrom  string          `json:"received_from_peer_id"`
		MessageHash   string          `json:"message_sha256"`
		MessageBase64 string          `json:"message_base64"`
		BlockIndex    int             `json:"block_index"`
		BlockHash     string          `json:"block_hash"`
		BlockPayload  json.RawMessage `json:"block_payload"`
	}
	if err := json.Unmarshal(line, &event); err != nil {
		t.Fatalf("decode emitted JSONL: %v; line=%s", err, line)
	}
	if event.Event != "gossip_message" || event.ObserverID != "observer-test-id" || event.ReceivedFrom != "delivery-peer-id" {
		t.Fatalf("unexpected event identity: %+v", event)
	}
	sum := sha256.Sum256(compressed)
	if event.MessageHash != hex.EncodeToString(sum[:]) {
		t.Fatalf("message hash = %q, want %x", event.MessageHash, sum)
	}
	gotCompressed, err := base64.StdEncoding.DecodeString(event.MessageBase64)
	if err != nil || !bytes.Equal(gotCompressed, compressed) {
		t.Fatalf("message_base64 did not preserve exact decrypted body: %v", err)
	}
	if event.BlockIndex != 7 || event.BlockHash != "block-hash-7" {
		t.Fatalf("unexpected block identity: index=%d hash=%q", event.BlockIndex, event.BlockHash)
	}
	if !bytes.Equal(event.BlockPayload, payload) {
		t.Fatalf("block payload was not preserved in full:\n got %s\nwant %s", event.BlockPayload, payload)
	}
	var decoded map[string]interface{}
	if err := json.Unmarshal(event.BlockPayload, &decoded); err != nil {
		t.Fatal(err)
	}
	storage := decoded["Storage"].(map[string]interface{})
	services := storage["services"].(map[string]interface{})
	worker := services["group_worker"].(map[string]interface{})
	details := worker["Details"].(map[string]interface{})
	if details["models"].([]interface{})[0] != "tiny-model" {
		t.Fatalf("nested ledger payload was lost: %+v", details)
	}
}

func TestDecodeGossipBlockTruncatesAtConfiguredLimit(t *testing.T) {
	plain := []byte(strings.Repeat("x", 1024*1024))
	decoded, truncated, err := decodeGossipBlock(gzipBytes(t, plain), 257)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !truncated {
		t.Fatal("expected decompressed body to be marked truncated")
	}
	if len(decoded) != 257 {
		t.Fatalf("decoded length = %d, want 257", len(decoded))
	}
}

func TestDecodeGossipBlockRejectsMalformedBody(t *testing.T) {
	if _, truncated, err := decodeGossipBlock([]byte("not a gzip message"), 4096); err == nil || truncated {
		t.Fatalf("malformed body returned truncated=%v, err=%v", truncated, err)
	}
}

func gzipBytes(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}
