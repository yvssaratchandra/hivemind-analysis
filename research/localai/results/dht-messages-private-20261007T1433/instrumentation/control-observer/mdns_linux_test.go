//go:build linux

package main

import (
	"encoding/binary"
	"net"
	"strings"
	"testing"
)

func TestDecodeDNSResponseWithP2PServiceRecords(t *testing.T) {
	packet := make([]byte, 12)
	binary.BigEndian.PutUint16(packet[2:4], 0x8400) // response, authoritative
	binary.BigEndian.PutUint16(packet[6:8], 3)      // three answers

	serviceName := wireDNSName("_localai._tcp.local")
	compressedInstance := append([]byte{8}, []byte("worker-1")...)
	compressedInstance = append(compressedInstance, 0xc0, 0x0c) // suffix pointer to serviceName at DNS offset 12
	packet = appendDNSRecordWire(packet, serviceName, 12, 1, 4500, compressedInstance)
	srvData := make([]byte, 6)
	binary.BigEndian.PutUint16(srvData[0:2], 0)
	binary.BigEndian.PutUint16(srvData[2:4], 0)
	binary.BigEndian.PutUint16(srvData[4:6], 12345)
	srvData = append(srvData, wireDNSName("worker-1.local")...)
	packet = appendDNSRecordWire(packet, compressedInstance, 33, 0x8001, 120, srvData)
	txt := []byte("network=alpha")
	txtData := append([]byte{byte(len(txt))}, txt...)
	packet = appendDNSRecordWire(packet, compressedInstance, 16, 0x8001, 120, txtData)

	decoded, err := decodeDNSMessage(packet)
	if err != nil {
		t.Fatalf("decode mDNS response: %v", err)
	}
	if !decoded.Response || len(decoded.Answers) != 3 {
		t.Fatalf("unexpected response shape: %+v", decoded)
	}
	if decoded.Answers[0].Type != "PTR" || decoded.Answers[0].Data != "worker-1._localai._tcp.local" {
		t.Fatalf("PTR record not decoded: %+v", decoded.Answers[0])
	}
	if decoded.Answers[1].Type != "SRV" || !strings.Contains(decoded.Answers[1].Data, "port=12345") || !strings.Contains(decoded.Answers[1].Data, "target=worker-1.local") {
		t.Fatalf("SRV record not decoded: %+v", decoded.Answers[1])
	}
	if decoded.Answers[2].Type != "TXT" || len(decoded.Answers[2].TXT) != 1 || decoded.Answers[2].TXT[0] != "network=alpha" {
		t.Fatalf("TXT record not decoded: %+v", decoded.Answers[2])
	}
}

func TestDecodeDNSRejectsMalformedQuestionNames(t *testing.T) {
	fixtures := []struct {
		name   string
		packet []byte
	}{
		{
			name:   "self-referential compression pointer",
			packet: []byte{0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0xc0, 0x0c},
		},
		{
			name:   "truncated compression pointer",
			packet: []byte{0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0xc0},
		},
	}
	for _, fixture := range fixtures {
		t.Run(fixture.name, func(t *testing.T) {
			if _, err := decodeDNSMessage(fixture.packet); err == nil {
				t.Fatal("malformed name unexpectedly parsed")
			}
		})
	}
}

func TestDecodeMDNSFrameFiltersUDPPort5353(t *testing.T) {
	dns := make([]byte, 12)
	frame := ipv4UDPFrame(49152, 5353, dns)
	packet, ok := decodeMDNSFrame(frame)
	if !ok || packet.SourcePort != 49152 || packet.DestPort != 5353 || len(packet.Payload) != len(dns) {
		t.Fatalf("mDNS frame was not decoded: ok=%v packet=%+v", ok, packet)
	}
	if _, ok := decodeMDNSFrame(ipv4UDPFrame(49152, 53, dns)); ok {
		t.Fatal("ordinary DNS UDP/53 packet passed the mDNS filter")
	}
}

func appendDNSRecord(packet []byte, name string, recordType, class uint16, ttl uint32, data []byte) []byte {
	return appendDNSRecordWire(packet, wireDNSName(name), recordType, class, ttl, data)
}

func appendDNSRecordWire(packet, owner []byte, recordType, class uint16, ttl uint32, data []byte) []byte {
	packet = append(packet, owner...)
	var header [10]byte
	binary.BigEndian.PutUint16(header[0:2], recordType)
	binary.BigEndian.PutUint16(header[2:4], class)
	binary.BigEndian.PutUint32(header[4:8], ttl)
	binary.BigEndian.PutUint16(header[8:10], uint16(len(data)))
	packet = append(packet, header[:]...)
	return append(packet, data...)
}

func wireDNSName(name string) []byte {
	var result []byte
	for _, label := range strings.Split(name, ".") {
		result = append(result, byte(len(label)))
		result = append(result, []byte(label)...)
	}
	return append(result, 0)
}

func ipv4UDPFrame(sourcePort, destinationPort uint16, payload []byte) []byte {
	frame := make([]byte, 14+20+8+len(payload))
	copy(frame[0:6], []byte{0, 1, 2, 3, 4, 5})
	copy(frame[6:12], []byte{6, 7, 8, 9, 10, 11})
	binary.BigEndian.PutUint16(frame[12:14], ethPIPv4)
	frame[14] = 0x45
	binary.BigEndian.PutUint16(frame[16:18], uint16(20+8+len(payload)))
	frame[22] = 64
	frame[23] = ipProtocolUDP
	copy(frame[26:30], net.IPv4(192, 0, 2, 10).To4())
	copy(frame[30:34], net.IPv4(224, 0, 0, 251).To4())
	udp := 14 + 20
	binary.BigEndian.PutUint16(frame[udp:udp+2], sourcePort)
	binary.BigEndian.PutUint16(frame[udp+2:udp+4], destinationPort)
	binary.BigEndian.PutUint16(frame[udp+4:udp+6], uint16(8+len(payload)))
	copy(frame[udp+8:], payload)
	return frame
}
