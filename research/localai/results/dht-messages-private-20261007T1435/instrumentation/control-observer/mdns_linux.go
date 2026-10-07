//go:build linux

package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"strings"
	"sync"
	"syscall"
)

const (
	ethPAll       = 0x0003
	ethPIPv4      = 0x0800
	ethPIPv6      = 0x86dd
	ethPVLAN      = 0x8100
	ethPQinQ      = 0x88a8
	ethPVLAN9100  = 0x9100
	ipProtocolUDP = 17
	mdnsPort      = 5353
)

type mdnsQuestion struct {
	Name  string `json:"name"`
	Type  string `json:"type"`
	Class uint16 `json:"class"`
}

type mdnsRecord struct {
	Name       string   `json:"name"`
	Type       string   `json:"type"`
	Class      uint16   `json:"class"`
	CacheFlush bool     `json:"cache_flush"`
	TTL        uint32   `json:"ttl"`
	Data       string   `json:"data,omitempty"`
	TXT        []string `json:"txt,omitempty"`
	RDataHex   string   `json:"rdata_hex,omitempty"`
}

type mdnsDecoded struct {
	ID         uint16         `json:"dns_id"`
	Response   bool           `json:"response"`
	Opcode     uint8          `json:"opcode"`
	Questions  []mdnsQuestion `json:"questions"`
	Answers    []mdnsRecord   `json:"answers"`
	Authority  []mdnsRecord   `json:"authority"`
	Additional []mdnsRecord   `json:"additional"`
}

type mdnsFrame struct {
	SourceMAC      string
	DestinationMAC string
	SourceIP       string
	DestinationIP  string
	SourcePort     uint16
	DestPort       uint16
	Payload        []byte
}

func captureMDNS(ctx context.Context, interfaceName string) {
	iface, err := net.InterfaceByName(interfaceName)
	if err != nil {
		emit(map[string]any{"event": "packet_capture_error", "category": "peer_discovery", "timestamp_utc": utcNow(), "observer_peer_id": currentPeerID(), "interface": interfaceName, "error": err.Error()})
		return
	}
	fd, err := syscall.Socket(syscall.AF_PACKET, syscall.SOCK_RAW, int(hostToNetwork16(ethPAll)))
	if err != nil {
		emit(map[string]any{"event": "packet_capture_error", "category": "peer_discovery", "timestamp_utc": utcNow(), "observer_peer_id": currentPeerID(), "interface": interfaceName, "error": err.Error(), "hint": "AF_PACKET requires CAP_NET_RAW in the observer container"})
		return
	}
	var closeOnce sync.Once
	closeSocket := func() { closeOnce.Do(func() { _ = syscall.Close(fd) }) }
	defer closeSocket()
	syscall.CloseOnExec(fd)
	_ = syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_RCVBUF, 4<<20)
	if err := syscall.Bind(fd, &syscall.SockaddrLinklayer{Protocol: hostToNetwork16(ethPAll), Ifindex: iface.Index}); err != nil {
		emit(map[string]any{"event": "packet_capture_error", "category": "peer_discovery", "timestamp_utc": utcNow(), "observer_peer_id": currentPeerID(), "interface": interfaceName, "error": err.Error()})
		return
	}
	emit(map[string]any{"event": "packet_capture_started", "category": "peer_discovery", "timestamp_utc": utcNow(), "observer_peer_id": currentPeerID(), "interface": interfaceName, "filter": "UDP source or destination port 5353 only", "capture_scope": "observer container network namespace"})

	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			closeSocket()
		case <-done:
		}
	}()
	defer close(done)

	buf := make([]byte, 65536)
	for {
		n, _, err := syscall.Recvfrom(fd, buf, 0)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if err == syscall.EINTR {
				continue
			}
			emit(map[string]any{"event": "packet_capture_error", "category": "peer_discovery", "timestamp_utc": utcNow(), "observer_peer_id": currentPeerID(), "interface": interfaceName, "error": err.Error()})
			return
		}
		frame := append([]byte(nil), buf[:n]...)
		packet, ok := decodeMDNSFrame(frame)
		if !ok {
			continue
		}
		decoded, parseErr := decodeDNSMessage(packet.Payload)
		event := map[string]any{
			"event":              "mdns_packet",
			"category":           "peer_discovery",
			"timestamp_utc":      utcNow(),
			"observer_peer_id":   currentPeerID(),
			"interface":          interfaceName,
			"source_mac":         packet.SourceMAC,
			"destination_mac":    packet.DestinationMAC,
			"source_ip":          packet.SourceIP,
			"destination_ip":     packet.DestinationIP,
			"source_port":        packet.SourcePort,
			"destination_port":   packet.DestPort,
			"dns_payload_size":   len(packet.Payload),
			"dns_payload_sha256": sha256Hex(packet.Payload),
			"dns_payload_hex":    hex.EncodeToString(packet.Payload),
		}
		if parseErr != nil {
			event["dns_parse_error"] = parseErr.Error()
		} else {
			event["dns"] = decoded
		}
		emit(event)
	}
}

func currentPeerID() string {
	value := observerPeerID.Load()
	if value == nil {
		return ""
	}
	return value.(string)
}

func decodeMDNSFrame(frame []byte) (mdnsFrame, bool) {
	var out mdnsFrame
	if len(frame) < 14 {
		return out, false
	}
	out.DestinationMAC = net.HardwareAddr(frame[0:6]).String()
	out.SourceMAC = net.HardwareAddr(frame[6:12]).String()
	offset := 14
	tetherType := uint16(frame[12])<<8 | uint16(frame[13])
	for tetherType == ethPVLAN || tetherType == ethPQinQ || tetherType == ethPVLAN9100 {
		if len(frame) < offset+4 {
			return out, false
		}
		tetherType = uint16(frame[offset+2])<<8 | uint16(frame[offset+3])
		offset += 4
	}
	var udpOffset, ipEnd int
	switch tetherType {
	case ethPIPv4:
		if len(frame) < offset+20 || frame[offset]>>4 != 4 {
			return out, false
		}
		headerLen := int(frame[offset]&0x0f) * 4
		totalLen := int(frame[offset+2])<<8 | int(frame[offset+3])
		fragment := uint16(frame[offset+6])<<8 | uint16(frame[offset+7])
		if headerLen < 20 || len(frame) < offset+headerLen || totalLen < headerLen+8 || fragment&0x1fff != 0 || frame[offset+9] != ipProtocolUDP {
			return out, false
		}
		ipEnd = offset + totalLen
		if ipEnd > len(frame) {
			ipEnd = len(frame)
		}
		out.SourceIP = net.IP(frame[offset+12 : offset+16]).String()
		out.DestinationIP = net.IP(frame[offset+16 : offset+20]).String()
		udpOffset = offset + headerLen
	case ethPIPv6:
		if len(frame) < offset+40 || frame[offset]>>4 != 6 {
			return out, false
		}
		payloadLen := int(frame[offset+4])<<8 | int(frame[offset+5])
		ipEnd = offset + 40 + payloadLen
		if ipEnd > len(frame) {
			ipEnd = len(frame)
		}
		next := frame[offset+6]
		udpOffset = offset + 40
		for hops := 0; hops < 8; hops++ {
			switch next {
			case 0, 43, 60: // Hop-by-hop, routing, destination options.
				if udpOffset+2 > ipEnd {
					return out, false
				}
				next, udpOffset = frame[udpOffset], udpOffset+(int(frame[udpOffset+1])+1)*8
			case 44: // Fragment header: only capture unfragmented/first fragments.
				if udpOffset+8 > ipEnd {
					return out, false
				}
				fragment := uint16(frame[udpOffset+2])<<8 | uint16(frame[udpOffset+3])
				if fragment&0xfff8 != 0 {
					return out, false
				}
				next, udpOffset = frame[udpOffset], udpOffset+8
			case 51: // Authentication header.
				if udpOffset+2 > ipEnd {
					return out, false
				}
				next, udpOffset = frame[udpOffset], udpOffset+(int(frame[udpOffset+1])+2)*4
			default:
				if next != ipProtocolUDP {
					return out, false
				}
				hops = 8
			}
		}
		out.SourceIP = net.IP(frame[offset+8 : offset+24]).String()
		out.DestinationIP = net.IP(frame[offset+24 : offset+40]).String()
	default:
		return out, false
	}
	if udpOffset+8 > ipEnd || udpOffset+8 > len(frame) {
		return out, false
	}
	out.SourcePort = uint16(frame[udpOffset])<<8 | uint16(frame[udpOffset+1])
	out.DestPort = uint16(frame[udpOffset+2])<<8 | uint16(frame[udpOffset+3])
	udpLen := int(frame[udpOffset+4])<<8 | int(frame[udpOffset+5])
	if (out.SourcePort != mdnsPort && out.DestPort != mdnsPort) || udpLen < 8 || udpOffset+udpLen > len(frame) {
		return out, false
	}
	payloadEnd := udpOffset + udpLen
	if payloadEnd > ipEnd {
		payloadEnd = ipEnd
	}
	out.Payload = frame[udpOffset+8 : payloadEnd]
	return out, true
}

func decodeDNSMessage(packet []byte) (mdnsDecoded, error) {
	var result mdnsDecoded
	if len(packet) < 12 {
		return result, fmt.Errorf("DNS header is truncated")
	}
	result.ID = uint16(packet[0])<<8 | uint16(packet[1])
	flags := uint16(packet[2])<<8 | uint16(packet[3])
	result.Response = flags&0x8000 != 0
	result.Opcode = uint8((flags >> 11) & 0x0f)
	qd := int(packet[4])<<8 | int(packet[5])
	ancount := int(packet[6])<<8 | int(packet[7])
	nscount := int(packet[8])<<8 | int(packet[9])
	arcount := int(packet[10])<<8 | int(packet[11])
	if qd+ancount+nscount+arcount > 1024 {
		return result, fmt.Errorf("unreasonable DNS section count")
	}
	offset := 12
	for i := 0; i < qd; i++ {
		name, next, err := dnsName(packet, offset)
		if err != nil || next+4 > len(packet) {
			return result, fmt.Errorf("invalid DNS question %d", i)
		}
		qtype := uint16(packet[next])<<8 | uint16(packet[next+1])
		qclass := uint16(packet[next+2])<<8 | uint16(packet[next+3])
		result.Questions = append(result.Questions, mdnsQuestion{Name: name, Type: dnsTypeName(qtype), Class: qclass & 0x7fff})
		offset = next + 4
	}
	var err error
	result.Answers, offset, err = readDNSRecords(packet, offset, ancount)
	if err != nil {
		return result, fmt.Errorf("invalid answers: %w", err)
	}
	result.Authority, offset, err = readDNSRecords(packet, offset, nscount)
	if err != nil {
		return result, fmt.Errorf("invalid authority records: %w", err)
	}
	result.Additional, _, err = readDNSRecords(packet, offset, arcount)
	if err != nil {
		return result, fmt.Errorf("invalid additional records: %w", err)
	}
	return result, nil
}

func readDNSRecords(packet []byte, offset, count int) ([]mdnsRecord, int, error) {
	result := make([]mdnsRecord, 0, count)
	for i := 0; i < count; i++ {
		name, next, err := dnsName(packet, offset)
		if err != nil || next+10 > len(packet) {
			return nil, offset, fmt.Errorf("record %d header is truncated", i)
		}
		rtype := uint16(packet[next])<<8 | uint16(packet[next+1])
		rclass := uint16(packet[next+2])<<8 | uint16(packet[next+3])
		ttl := uint32(packet[next+4])<<24 | uint32(packet[next+5])<<16 | uint32(packet[next+6])<<8 | uint32(packet[next+7])
		rdlen := int(packet[next+8])<<8 | int(packet[next+9])
		rdataOffset := next + 10
		end := rdataOffset + rdlen
		if end > len(packet) {
			return nil, offset, fmt.Errorf("record %d rdata is truncated", i)
		}
		rdata := packet[rdataOffset:end]
		record := mdnsRecord{Name: name, Type: dnsTypeName(rtype), Class: rclass & 0x7fff, CacheFlush: rclass&0x8000 != 0, TTL: ttl}
		switch rtype {
		case 1:
			if len(rdata) == net.IPv4len {
				record.Data = net.IP(rdata).String()
			}
		case 2, 5, 12:
			if value, _, nameErr := dnsName(packet, rdataOffset); nameErr == nil {
				record.Data = value
			}
		case 16:
			record.TXT = decodeTXT(rdata)
		case 28:
			if len(rdata) == net.IPv6len {
				record.Data = net.IP(rdata).String()
			}
		case 33:
			if len(rdata) >= 7 {
				priority := uint16(rdata[0])<<8 | uint16(rdata[1])
				weight := uint16(rdata[2])<<8 | uint16(rdata[3])
				port := uint16(rdata[4])<<8 | uint16(rdata[5])
				target, _, nameErr := dnsName(packet, rdataOffset+6)
				if nameErr == nil {
					record.Data = fmt.Sprintf("priority=%d weight=%d port=%d target=%s", priority, weight, port, target)
				}
			}
		default:
			record.RDataHex = hex.EncodeToString(rdata)
		}
		result = append(result, record)
		offset = end
	}
	return result, offset, nil
}

func decodeTXT(data []byte) []string {
	result := []string{}
	for offset := 0; offset < len(data); {
		length := int(data[offset])
		offset++
		if offset+length > len(data) {
			return append(result, "<malformed>")
		}
		result = append(result, string(data[offset:offset+length]))
		offset += length
	}
	return result
}

func dnsName(packet []byte, offset int) (string, int, error) {
	labels := make([]string, 0, 4)
	position := offset
	next := -1
	for hops := 0; hops < 128; hops++ {
		if position >= len(packet) {
			return "", 0, fmt.Errorf("DNS name exceeds packet")
		}
		length := int(packet[position])
		if length&0xc0 == 0xc0 {
			if position+1 >= len(packet) {
				return "", 0, fmt.Errorf("truncated DNS compression pointer")
			}
			pointer := (length&0x3f)<<8 | int(packet[position+1])
			if pointer >= len(packet) {
				return "", 0, fmt.Errorf("DNS compression pointer outside packet")
			}
			if next < 0 {
				next = position + 2
			}
			position = pointer
			continue
		}
		if length&0xc0 != 0 {
			return "", 0, fmt.Errorf("invalid DNS label type")
		}
		position++
		if length == 0 {
			if next < 0 {
				next = position
			}
			return strings.Join(labels, "."), next, nil
		}
		if length > 63 || position+length > len(packet) {
			return "", 0, fmt.Errorf("invalid DNS label length")
		}
		labels = append(labels, string(packet[position:position+length]))
		position += length
	}
	return "", 0, fmt.Errorf("DNS compression loop")
}

func dnsTypeName(value uint16) string {
	switch value {
	case 1:
		return "A"
	case 2:
		return "NS"
	case 5:
		return "CNAME"
	case 12:
		return "PTR"
	case 16:
		return "TXT"
	case 28:
		return "AAAA"
	case 33:
		return "SRV"
	case 255:
		return "ANY"
	default:
		return fmt.Sprintf("TYPE%d", value)
	}
}

func hostToNetwork16(value uint16) uint16 { return value<<8 | value>>8 }

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return fmt.Sprintf("%x", sum[:])
}
