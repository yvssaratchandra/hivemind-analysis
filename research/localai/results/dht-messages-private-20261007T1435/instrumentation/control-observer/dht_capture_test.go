package main

import (
	"encoding/json"
	"strings"
	"testing"

	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

const fixturePeerID = "12D3KooWSgjTr1c2V7TemegMjtxi2HzmK3AaUfaXBSuDTDb5GojR"

func TestBootstrapPeerRequiresLiteralPrivateIPAndPeerID(t *testing.T) {
	valid := []string{
		"/ip4/172.18.0.2/tcp/4001/p2p/" + fixturePeerID,
		"/ip4/172.18.0.2/udp/4001/quic-v1/p2p/" + fixturePeerID,
		"/ip4/127.0.0.1/tcp/1/p2p/" + fixturePeerID,
	}
	for _, raw := range valid {
		t.Run(raw, func(t *testing.T) {
			addr, err := ma.NewMultiaddr(raw)
			if err != nil {
				t.Fatalf("fixture multiaddr invalid: %v", err)
			}
			if err := validatePrivateBootstrapAddr(addr); err != nil {
				t.Fatalf("private bootstrap address rejected: %v", err)
			}
		})
	}
	invalid := []string{
		"/ip4/8.8.8.8/tcp/4001/p2p/" + fixturePeerID,
		"/dns4/bootstrap.libp2p.io/tcp/443/p2p/" + fixturePeerID,
		"/ip4/172.18.0.2/tcp/4001",
		"/ip4/172.18.0.2/udp/4001",
	}
	for _, raw := range invalid {
		t.Run(raw, func(t *testing.T) {
			addr, err := ma.NewMultiaddr(raw)
			if err != nil {
				t.Fatalf("fixture multiaddr invalid: %v", err)
			}
			if err := validatePrivateBootstrapAddr(addr); err == nil {
				t.Fatalf("unsafe or incomplete bootstrap address accepted: %s", raw)
			}
		})
	}
}

func TestPrivateBootstrapSentinelIsLoopbackAndValid(t *testing.T) {
	addr, err := privateBootstrapSentinel()
	if err != nil {
		t.Fatalf("create sentinel: %v", err)
	}
	if err := validatePrivateBootstrapAddr(addr); err != nil {
		t.Fatalf("sentinel is not a valid private bootstrap: %v", err)
	}
	if !strings.HasPrefix(addr.String(), "/ip4/127.0.0.1/tcp/1/p2p/") {
		t.Fatalf("sentinel is not loopback: %s", addr)
	}
}

func TestDHTModeNamesMapToPinnedKadModes(t *testing.T) {
	for _, name := range []string{"server", "auto", "auto-server", "client"} {
		if _, err := dhtModeValue(name); err != nil {
			t.Errorf("supported mode %q rejected: %v", name, err)
		}
	}
	if _, err := dhtModeValue("leader"); err == nil {
		t.Fatal("unknown mode was accepted")
	}
}

func TestDHTMessageMetadataIncludesKeyAndPeerLists(t *testing.T) {
	peerAddr, err := ma.NewMultiaddr("/ip4/172.18.0.5/tcp/4001")
	if err != nil {
		t.Fatal(err)
	}
	peerID, err := peer.Decode(fixturePeerID)
	if err != nil {
		t.Fatal(err)
	}
	message := &pb.Message{
		Type: pb.Message_GET_PROVIDERS,
		Key:  []byte("private-rendezvous-key"),
		CloserPeers: []*pb.Message_Peer{{
			Id:         []byte(peerID),
			Addrs:      [][]byte{peerAddr.Bytes()},
			Connection: pb.Message_CONNECTED,
		}},
	}
	event := make(map[string]any)
	addDHTMessageMetadata(event, message)
	encoded, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(encoded, &got); err != nil {
		t.Fatal(err)
	}
	if got["message_type"] != "GET_PROVIDERS" || got["key_utf8"] != "private-rendezvous-key" {
		t.Fatalf("message type/key metadata missing: %+v", got)
	}
	peers := got["closer_peers"].([]any)
	peerData := peers[0].(map[string]any)
	if peerData["peer_id"] != fixturePeerID || peerData["connection_type"] != "CONNECTED" {
		t.Fatalf("peer metadata missing: %+v", peerData)
	}
	if peerData["addrs"].([]any)[0] != peerAddr.String() {
		t.Fatalf("peer multiaddr not preserved: %+v", peerData["addrs"])
	}
}
