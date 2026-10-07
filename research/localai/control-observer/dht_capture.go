package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"unicode/utf8"

	kaddht "github.com/libp2p/go-libp2p-kad-dht"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	libcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"google.golang.org/protobuf/proto"
)

var dhtEventSequence atomic.Uint64

type stringListFlag []string

func (f *stringListFlag) String() string { return strings.Join(*f, ",") }

func (f *stringListFlag) Set(value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("must not be empty")
	}
	*f = append(*f, value)
	return nil
}

func (f stringListFlag) parse() ([]ma.Multiaddr, error) {
	addrs := make([]ma.Multiaddr, 0, len(f))
	seen := make(map[string]struct{}, len(f))
	for _, raw := range f {
		addr, err := ma.NewMultiaddr(raw)
		if err != nil {
			return nil, fmt.Errorf("%q is not a multiaddr: %w", raw, err)
		}
		if err := validatePrivateBootstrapAddr(addr); err != nil {
			return nil, fmt.Errorf("%q: %w", raw, err)
		}
		if _, ok := seen[addr.String()]; ok {
			return nil, fmt.Errorf("duplicate address %q", raw)
		}
		seen[addr.String()] = struct{}{}
		addrs = append(addrs, addr)
	}
	return addrs, nil
}

func validatePrivateBootstrapAddr(addr ma.Multiaddr) error {
	if _, err := peer.AddrInfoFromP2pAddr(addr); err != nil {
		return fmt.Errorf("must include a valid /p2p/<peer-id> component: %w", err)
	}
	ipValue, err := addr.ValueForProtocol(ma.P_IP4)
	if err != nil {
		ipValue, err = addr.ValueForProtocol(ma.P_IP6)
	}
	if err != nil {
		return fmt.Errorf("must use a literal private IP address (DNS and public bootstrap addresses are disallowed)")
	}
	ip := net.ParseIP(ipValue)
	if ip == nil || (!ip.IsPrivate() && !ip.IsLoopback()) {
		return fmt.Errorf("IP address must be private or loopback; public bootstrap addresses are disallowed")
	}
	if _, err := addr.ValueForProtocol(ma.P_TCP); err != nil {
		if _, udpErr := addr.ValueForProtocol(ma.P_UDP); udpErr != nil {
			return fmt.Errorf("must include a TCP or UDP/quic-v1 transport")
		}
		if _, quicErr := addr.ValueForProtocol(ma.P_QUIC_V1); quicErr != nil {
			return fmt.Errorf("UDP bootstrap transport must include /quic-v1")
		}
	}
	return nil
}

func dhtModeValue(name string) (kaddht.ModeOpt, error) {
	switch name {
	case "server":
		return kaddht.ModeServer, nil
	case "auto":
		return kaddht.ModeAuto, nil
	case "auto-server":
		return kaddht.ModeAutoServer, nil
	case "client":
		return kaddht.ModeClient, nil
	default:
		return 0, fmt.Errorf("unknown DHT mode %q", name)
	}
}

func privateBootstrapSentinel() (ma.Multiaddr, error) {
	key, _, err := libcrypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return nil, err
	}
	id, err := peer.IDFromPrivateKey(key)
	if err != nil {
		return nil, err
	}
	return ma.NewMultiaddr("/ip4/127.0.0.1/tcp/1/p2p/" + id.String())
}

// logInboundDHTRequest uses the pinned kad-dht OnRequestHook. The public hook
// exposes a decoded protobuf Message but not the original varint frame or the
// response generated after this callback. The logged bytes are therefore a
// canonical protobuf re-encoding, explicitly distinguished from a raw capture.
func logInboundDHTRequest(_ context.Context, stream network.Stream, req *pb.Message) {
	if req == nil || stream == nil {
		return
	}
	body, err := proto.Marshal(req)
	if err != nil {
		emit(map[string]any{
			"event": "dht_rpc_error", "category": "dht_rpc", "timestamp_utc": utcNow(),
			"observer_peer_id": currentPeerID(), "from_peer_id": stream.Conn().RemotePeer().String(),
			"remote_multiaddr": stream.Conn().RemoteMultiaddr().String(), "protocol_id": string(stream.Protocol()),
			"error": "re-encode inbound DHT protobuf message: " + err.Error(),
		})
		return
	}
	event := map[string]any{
		"event":                    "dht_message",
		"category":                 "dht_rpc",
		"timestamp_utc":            utcNow(),
		"direction":                "inbound_request",
		"capture_fidelity":         "decoded_protobuf_message_reencoded_by_hook",
		"event_id":                 fmt.Sprintf("dht-%d", dhtEventSequence.Add(1)),
		"observer_peer_id":         currentPeerID(),
		"from_peer_id":             stream.Conn().RemotePeer().String(),
		"to_peer_id":               currentPeerID(),
		"remote_multiaddr":         stream.Conn().RemoteMultiaddr().String(),
		"protocol_id":              string(stream.Protocol()),
		"protobuf_body_base64":     base64.StdEncoding.EncodeToString(body),
		"protobuf_body_size_bytes": len(body),
	}
	bodyHash := sha256.Sum256(body)
	event["protobuf_body_sha256"] = hex.EncodeToString(bodyHash[:])
	addDHTMessageMetadata(event, req)
	event["schema"] = schemaVersion
	emit(event)
}

func addDHTMessageMetadata(event map[string]any, msg *pb.Message) {
	if msg == nil {
		return
	}
	event["message_type"] = msg.GetType().String()
	event["message_type_code"] = int32(msg.GetType())
	event["key_base64"] = base64.StdEncoding.EncodeToString(msg.GetKey())
	event["key_hex"] = hex.EncodeToString(msg.GetKey())
	keyHash := sha256.Sum256(msg.GetKey())
	event["key_sha256"] = hex.EncodeToString(keyHash[:])
	if utf8.Valid(msg.GetKey()) {
		event["key_utf8"] = string(msg.GetKey())
	}
	event["closer_peers"] = dhtPeerMetadata(msg.GetCloserPeers())
	event["provider_peers"] = dhtPeerMetadata(msg.GetProviderPeers())
	if record := msg.GetRecord(); record != nil {
		event["record"] = map[string]any{
			"key_base64":   base64.StdEncoding.EncodeToString(record.GetKey()),
			"value_base64": base64.StdEncoding.EncodeToString(record.GetValue()),
		}
	}
}

func dhtPeerMetadata(peers []*pb.Message_Peer) []map[string]any {
	result := make([]map[string]any, 0, len(peers))
	for _, item := range peers {
		if item == nil {
			continue
		}
		addresses := make([]string, 0, len(item.GetAddrs()))
		for _, raw := range item.GetAddrs() {
			addr, err := ma.NewMultiaddrBytes(raw)
			if err != nil {
				addresses = append(addresses, "invalid:"+hex.EncodeToString(raw))
				continue
			}
			addresses = append(addresses, addr.String())
		}
		result = append(result, map[string]any{
			"peer_id":         peer.ID(item.GetId()).String(),
			"addrs":           addresses,
			"connection_type": item.GetConnection().String(),
		})
	}
	return result
}
