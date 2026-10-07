// control-observer joins a private LocalAI EdgeVPN network as a passive
// ledger observer. It does not publish LocalAI service, model, or health
// records. EdgeVPN still synchronizes this node's own ledger, so the observer
// is a network participant and its peer ID is recorded in every event.
package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/mudler/edgevpn/pkg/blockchain"
	"github.com/mudler/edgevpn/pkg/config"
	"github.com/mudler/edgevpn/pkg/hub"
	"github.com/mudler/edgevpn/pkg/node"
	ma "github.com/multiformats/go-multiaddr"
)

const schemaVersion = "localai-control-observer/v1"

var (
	maxMessageSize      = 64 << 20
	defaultListenAddrs  = "/ip4/0.0.0.0/tcp/0,/ip4/0.0.0.0/udp/0/quic-v1"
	jsonEncoderMu       sync.Mutex
	observerPeerID      atomic.Value
	decompressedMaxFlag = flag.Int("max-decompressed-bytes", maxMessageSize, "maximum decompressed gossip body to decode and log")
	captureMDNSFlag     = flag.Bool("capture-mdns", true, "capture UDP/5353 packets on the observer container interface")
	interfaceFlag       = flag.String("interface", "eth0", "container interface for UDP/5353 packet capture")
	runForFlag          = flag.Duration("run-for", 0, "optional runtime limit (0 runs until SIGTERM/SIGINT)")
)

type blockHeader struct {
	Index    int    `json:"Index"`
	Hash     string `json:"Hash"`
	PrevHash string `json:"PrevHash"`
}

func main() {
	flag.Parse()
	observerPeerID.Store("")
	if *decompressedMaxFlag < 1024 {
		fatal("-max-decompressed-bytes must be at least 1024")
	}

	tokenEnv := envOr("OBSERVER_TOKEN_ENV", "LOCALAI_P2P_TOKEN")
	token := os.Getenv(tokenEnv)
	if token == "" {
		fatal("missing EdgeVPN token in environment variable %s", tokenEnv)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if *runForFlag > 0 {
		var durationCancel context.CancelFunc
		ctx, durationCancel = context.WithTimeout(ctx, *runForFlag)
		defer durationCancel()
	}

	if *captureMDNSFlag {
		go captureMDNS(ctx, *interfaceFlag)
	} else {
		emit(map[string]any{"event": "packet_capture_disabled", "category": "peer_discovery", "timestamp_utc": utcNow()})
	}

	n, err := newObserverNode(token)
	if err != nil {
		fatal("configure EdgeVPN observer: %v", err)
	}
	if err := n.Start(ctx); err != nil {
		fatal("start EdgeVPN observer: %v", err)
	}
	id := n.Host().ID().String()
	observerPeerID.Store(id)
	emit(map[string]any{
		"event":                   "observer_started",
		"category":                "startup",
		"schema":                  schemaVersion,
		"timestamp_utc":           utcNow(),
		"observer_peer_id":        id,
		"role":                    "passive_ledger_observer",
		"dht_enabled":             false,
		"mdns_enabled":            true,
		"localai_records_added":   false,
		"edgevpn_ledger_sync":     "enabled; node.Start periodically publishes this observer's ledger snapshot",
		"stream_events_available": false,
		"stream_event_note":       "libp2p v0.48.0 Network.Notifiee reports listen and connection events, but has no stream callbacks",
	})

	notifiee := &network.NotifyBundle{
		ListenF: func(_ network.Network, addr ma.Multiaddr) {
			emit(map[string]any{"event": "listen_address", "category": "startup", "timestamp_utc": utcNow(), "observer_peer_id": id, "state": "opened", "multiaddr": addr.String()})
		},
		ListenCloseF: func(_ network.Network, addr ma.Multiaddr) {
			emit(map[string]any{"event": "listen_address", "category": "shutdown", "timestamp_utc": utcNow(), "observer_peer_id": id, "state": "closed", "multiaddr": addr.String()})
		},
		ConnectedF: func(_ network.Network, conn network.Conn) {
			logConnection("connected", id, conn)
		},
		DisconnectedF: func(_ network.Network, conn network.Conn) {
			logConnection("disconnected", id, conn)
		},
	}
	n.Host().Network().Notify(notifiee)
	defer func() {
		_ = n.Host().Close()
		n.Host().Network().StopNotify(notifiee)
	}()
	for _, addr := range n.Host().Addrs() {
		emit(map[string]any{"event": "listen_address", "category": "startup", "timestamp_utc": utcNow(), "observer_peer_id": id, "state": "active_at_observer_registration", "multiaddr": addr.String()})
	}
	for _, conn := range n.Host().Network().Conns() {
		logConnection("already_connected_at_observer_registration", id, conn)
	}

	<-ctx.Done()
	emit(map[string]any{"event": "observer_stopping", "category": "shutdown", "timestamp_utc": utcNow(), "observer_peer_id": id, "reason": context.Cause(ctx).Error()})
}

// newObserverNode uses the same EdgeVPN token decoder and discovery interval
// as LocalAI's core/p2p newNodeOpts: mDNS on, DHT off, 10-second ledger sync.
// It intentionally omits services.Alive and all service registrations.
func newObserverNode(token string) (*node.Node, error) {
	listen := strings.Split(envOr("LOCALAI_P2P_LISTEN_MADDRS", defaultListenAddrs), ",")
	cfg := config.Config{
		NetworkToken:   token,
		ListenMaddrs:   listen,
		LogLevel:       "fatal",
		Libp2pLogLevel: "fatal",
		Limit: config.ResourceLimit{
			Enable:   true,
			MaxConns: 100,
		},
		Ledger: config.Ledger{
			AnnounceInterval: 10 * time.Second,
			SyncInterval:     10 * time.Second,
		},
		Discovery: config.Discovery{
			DHT:      false,
			MDNS:     true,
			Interval: 10 * time.Second,
		},
		Connection: config.Connection{
			HolePunch:      true,
			AutoRelay:      false,
			MaxConnections: 1000,
			RelayService: config.RelayService{
				Disabled: true,
			},
		},
		NAT: config.NAT{
			Service:           true,
			Map:               true,
			RateLimit:         true,
			RateLimitGlobal:   100,
			RateLimitPeer:     100,
			RateLimitInterval: 10 * time.Second,
		},
	}
	opts, _, err := cfg.ToOpts(nil)
	if err != nil {
		return nil, err
	}
	opts = append(opts, node.Handlers(logGossipMessage{maxDecompressed: int64(*decompressedMaxFlag)}.handle))
	return node.New(opts...)
}

type logGossipMessage struct {
	maxDecompressed int64
}

func (h logGossipMessage) handle(_ *blockchain.Ledger, msg *hub.Message, _ chan *hub.Message) error {
	if msg == nil {
		return nil
	}
	raw := []byte(msg.Message)
	sum := sha256.Sum256(raw)
	event := map[string]any{
		"event":                          "gossip_message",
		"category":                       "ledger_sync",
		"schema":                         schemaVersion,
		"timestamp_utc":                  utcNow(),
		"observer_peer_id":               observerPeerID.Load().(string),
		"received_from_peer_id":          msg.SenderID,
		"edgevpn_sender_id":              msg.SenderID,
		"message_sha256":                 hex.EncodeToString(sum[:]),
		"message_sha256_scope":           "decrypted EdgeVPN body bytes before gzip decoding",
		"message_size_bytes":             len(raw),
		"message_base64":                 base64.StdEncoding.EncodeToString(raw),
		"edgevpn_annotations":            msg.Annotations,
		"message_sender_identity_caveat": "EdgeVPN v0.34.0 replaces Message.SenderID with libp2p gossipsub ReceivedFrom; this is the immediate delivery peer, which may be a relay",
	}
	decoded, truncated, err := decodeGossipBlock(raw, h.maxDecompressed)
	if err != nil {
		event["decode_error"] = err.Error()
	} else {
		var header blockHeader
		if err := json.Unmarshal(decoded, &header); err == nil {
			event["block_index"] = header.Index
			event["block_hash"] = header.Hash
			event["block_prev_hash"] = header.PrevHash
			event["message_identity"] = fmt.Sprintf("block:%d:%s", header.Index, header.Hash)
			event["block_payload"] = json.RawMessage(decoded)
		} else {
			event["decode_error"] = fmt.Sprintf("decompressed body is not a ledger block: %v", err)
		}
	}
	if truncated {
		event["payload_truncated"] = true
		event["payload_truncation_reason"] = "decompressed body exceeded configured safety limit"
	}
	emit(event)
	return nil
}

func decodeGossipBlock(raw []byte, max int64) ([]byte, bool, error) {
	r, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		return nil, false, fmt.Errorf("gzip: %w", err)
	}
	defer r.Close()
	decoded, err := io.ReadAll(io.LimitReader(r, max+1))
	if err != nil {
		return nil, false, fmt.Errorf("read gzip body: %w", err)
	}
	if int64(len(decoded)) > max {
		return decoded[:max], true, nil
	}
	return decoded, false, nil
}

func logConnection(state, observerID string, conn network.Conn) {
	if conn == nil {
		return
	}
	stat := conn.Stat()
	emit(map[string]any{
		"event":              "peer_connection",
		"category":           "connectivity",
		"timestamp_utc":      utcNow(),
		"observer_peer_id":   observerID,
		"state":              state,
		"action":             state,
		"remote_peer_id":     conn.RemotePeer().String(),
		"connection_id":      conn.ID(),
		"direction":          stat.Direction.String(),
		"local_multiaddr":    conn.LocalMultiaddr().String(),
		"remote_multiaddr":   conn.RemoteMultiaddr().String(),
		"limited_connection": stat.Limited,
	})
}

func emit(event map[string]any) {
	event["schema"] = schemaVersion
	jsonEncoderMu.Lock()
	defer jsonEncoderMu.Unlock()
	enc := json.NewEncoder(os.Stdout)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(event); err != nil {
		fmt.Fprintf(os.Stderr, "write event: %v\n", err)
	}
}

func utcNow() string { return time.Now().UTC().Format(time.RFC3339Nano) }

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(2)
}
