package peer

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log"
	"math/big"
	"sort"

	"context"
	"encoding/hex"
	"encoding/json"

	//"io"

	"final/network/RelayFinal/pkg/relay/models"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/client"
	"github.com/libp2p/go-libp2p/p2p/protocol/holepunch"
	"github.com/libp2p/go-libp2p/p2p/protocol/identify"
	"github.com/multiformats/go-multiaddr"

	"github.com/libp2p/go-libp2p/p2p/transport/websocket"

	//webtransport "github.com/libp2p/go-libp2p/p2p/transport/webtransport"
	libp2ptls "github.com/libp2p/go-libp2p/p2p/security/tls"
)

const UserPeerProtocol = protocol.ID("/depth/1.0.0")

var OwnPubIP string

// type RelayDist struct {
// 	relayID string
// 	dist    *big.Int
// }

func NewPeer(relayMultiAddrList []string, peerType string) (*models.UserPeer, error) {

	// extracts relay peerIDs by splitting
	var relayList []string
	for _, multiaddr := range relayMultiAddrList {
		parts := strings.Split(multiaddr, "/")
		relayList = append(relayList, parts[len(parts)-1])
	}

	caCertPool := x509.NewCertPool()

	log.Println("[DEBUG] Creating connection manager")
	connMgr, err := connmgr.NewConnManager(100, 400)
	if err != nil {
		log.Println("[DEBUG] Failed to create connection manager:", err)
		return nil, err
	}

	tlsConfig := &tls.Config{
		RootCAs:            caCertPool,
		InsecureSkipVerify: true,
		// Other TLS configurations like ClientAuth, InsecureSkipVerify, etc.
	}

	log.Println("[DEBUG] Creating libp2p Host")
	// this is the libp2p host that will handle peers
	h, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0/ws"), // WebSocket
		libp2p.Security(libp2ptls.ID, libp2ptls.New),
		libp2p.ConnectionManager(connMgr),
		libp2p.EnableNATService(),
		libp2p.EnableRelay(),
		libp2p.Transport(websocket.New, websocket.WithTLSConfig(tlsConfig)),
		// libp2p.Transport(websocket.NewWithTLSConfig(tlsConfig)),
		// libp2p.Transport(websocket.New),
	)
	if err != nil {
		log.Println("[DEBUG] Failed to create Host:", err)
		return nil, err
	}

	log.Println("[DEBUG] Creating identify service")
	idSvc, err := identify.NewIDService(h)
	if err != nil {
		log.Println("[DEBUG] Failed to create identify service:", err)
		h.Close()
		return nil, err
	}

	getListenAddrs := func() []multiaddr.Multiaddr {
		var publicAddrs []multiaddr.Multiaddr
		for _, addr := range h.Addrs() {
			if !isPrivateAddr(addr) {
				publicAddrs = append(publicAddrs, addr)
			}
		}
		return publicAddrs
	}

	log.Println("[DEBUG] Initializing hole punching service")
	hps, err := holepunch.NewService(h, idSvc, getListenAddrs)
	if err != nil {
		log.Println("[DEBUG] Failed to create hole punching service:", err)
		h.Close()
		return nil, err
	}
	//hps is never explicitly used but runs in the bg
	_ = hps

	//next code calculates XOR Dist. w.r.t all relay peers to choose closest relay
	var distmap []RelayDist
	//OwnPubIP = GetPublicIP()
	h1 := sha256.New()
	h1.Write([]byte(h.ID().String()))
	peerIDhash := hex.EncodeToString(h1.Sum(nil))

	for _, relay := range relayList {

		h_R := sha256.New()
		h_R.Write([]byte(relay))
		RelayIDhash := hex.EncodeToString(h_R.Sum(nil))

		dist := XorHexToBigInt(peerIDhash, RelayIDhash)
		distmap = append(distmap, RelayDist{dist: dist, relayID: relay})
	}

	sort.Slice(distmap, func(i, j int) bool {
		return distmap[i].dist.Cmp(distmap[j].dist) < 0
	})

	relayIDused := distmap[0].relayID
	var RelayAddr string

	for _, multiaddr := range relayMultiAddrList {
		parts := strings.Split(multiaddr, "/")
		if parts[len(parts)-1] == relayIDused {
			RelayAddr = multiaddr
			break
		}
	}

	log.Println("[DEBUG] Parsing relay address:", RelayAddr)
	relayMA, err := multiaddr.NewMultiaddr(RelayAddr)
	if err != nil {
		log.Println("[DEBUG] Failed to parse relay multiaddr:", err)
		return nil, err
	}

	// converts multiaddr to relay info
	relayInfo, err := peer.AddrInfoFromP2pAddr(relayMA)
	if err != nil {
		log.Println("[DEBUG] Failed to extract relay peer info:", err)
		return nil, err
	}

	// Create circuit relay client
	log.Println("[DEBUG] Creating circuit relay client")
	// _ = client // Import for reservation function

	dp := &models.UserPeer{
		Host:      h,
		RelayAddr: relayMA,
		RelayID:   relayInfo.ID,
		Peers:     make(map[peer.ID]string),
	}

	log.Println(h.ID().String())

	switch peerType {
	case "depth":
		log.Println("[DEBUG] Setting stream handler for Depth protocol")
		h.SetStreamHandler(UserPeerProtocol, handleDepthStream)
	case "user":
		log.Println("[DEBUG] Peer type is 'user'. This peer will act as a client and will not serve incoming requests.")
    default:
        log.Printf("[WARN] Unknown peer type: %s. Not setting a stream handler.", peerType)
    }
	return dp, nil
}

func isPrivateAddr(addr multiaddr.Multiaddr) bool {
	addrStr := addr.String()
	return strings.Contains(addrStr, "127.0.0.1") ||
		strings.Contains(addrStr, "192.168.") ||
		strings.Contains(addrStr, "10.") ||
		strings.Contains(addrStr, "172.16.") ||
		strings.Contains(addrStr, "172.17.") ||
		strings.Contains(addrStr, "172.18.") ||
		strings.Contains(addrStr, "172.19.") ||
		strings.Contains(addrStr, "172.2") ||
		strings.Contains(addrStr, "172.30.") ||
		strings.Contains(addrStr, "172.31.")
}

func Start(dp *models.UserPeer, ctx context.Context) error {
	log.Println("[DEBUG] Connecting to relay:", dp.RelayAddr)
	relayInfo, _ := peer.AddrInfoFromP2pAddr(dp.RelayAddr)
	if err := dp.Host.Connect(ctx, *relayInfo); err != nil {
		log.Println("[DEBUG] Failed to connect to relay:", err)
		return fmt.Errorf("failed to connect to relay: %w", err)
	}

	// Make reservation with the relay
	log.Println("[DEBUG] Making reservation with relay...")
	reservation, err := client.Reserve(ctx, dp.Host, *relayInfo)
	if err != nil {
		log.Printf("[DEBUG] Failed to make reservation: %v\n", err)
		return fmt.Errorf("failed to make reservation: %w", err)
	}
	log.Printf("[DEBUG] Reservation successful! Expiry: %v\n", reservation.Expiration)

	log.Printf("[DEBUG] Peer Started! \n Peer ID: %s\n", dp.Host.ID())

	//prints all listen addresses of host
	for _, addr := range dp.Host.Addrs() {
		log.Printf("[DEBUG] Address: %s/p2p/%s\n", addr, dp.Host.ID())
	}

	// creates circuitAddr to talk with relays
	circuitAddr := dp.RelayAddr.Encapsulate(
		multiaddr.StringCast(fmt.Sprintf("/p2p-circuit/p2p/%s", dp.Host.ID())))

	log.Printf("[INFO] Circuit Address (share this with other peers): %s\n", circuitAddr)

	// Start a goroutine to periodically refresh reservations
	go refreshReservations(dp, ctx, *relayInfo)
    //now, sends a register req. to relay
    var reqSent models.ReqFormat
    reqSent.Type = "register"
    reqSent.PeerID = dp.Host.ID().String()
    reqSent.ReqParams = json.RawMessage("{}")
    reqSent.Body = json.RawMessage("{}")
    
    log.Printf("reqSent before marshal: %+v", reqSent)
    log.Printf("reqSent PID in peer.go: %s", reqSent.PeerID)

    stream, err := dp.Host.NewStream(context.Background(), relayInfo.ID, UserPeerProtocol)
    if err != nil {
        log.Printf("[DEBUG]Error Opening stream to relay: %v", err)
        return err
    }

    log.Println("[DEBUG]Opened stream to relay successsfully")
    reqJson, err := json.Marshal(reqSent)
    if err != nil {
        log.Printf("[DEBUG]Error marshalling the req to be sent: %v", err)
        return err
    }
    
    log.Printf("[DEBUG]Marshaled JSON: %s", string(reqJson))

    _, err = stream.Write(reqJson)
    if err != nil {
        log.Printf("error during writing to stream: %v", err.Error())
        return err
    }
    time.Sleep(1 * time.Second)

    stream.Close()
    return nil
}

// func to refresh relay reservations
func refreshReservations(dp *models.UserPeer, ctx context.Context, relayInfo peer.AddrInfo) {
	ticker := time.NewTicker(5 * time.Minute) // Refresh every 5 minutes
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			log.Println("[DEBUG] Refreshing relay reservation...")

			if reservation, err := client.Reserve(ctx, dp.Host, relayInfo); err != nil {
				log.Printf("[DEBUG] Failed to refresh reservation: %v\n", err)
			} else {
				log.Printf("[DEBUG] Reservation refreshed! Expiry: %v\n", reservation.Expiration)
			}
		case <-ctx.Done():
			return
		}
	}
}

func handleDepthStream(s network.Stream) {
    log.Println("[DEBUG] Incoming Depth stream from", s.Conn().RemotePeer())
    defer s.Close()

    reader := bufio.NewReader(s)
    for {
        line, err := reader.ReadBytes('\n') // newline-delimited framing
        if err != nil {
            // If we got no bytes, stop. If we got some bytes + err, process the bytes then stop.
            if len(line) == 0 {
                if errors.Is(err, io.EOF) {
                    log.Println("[DEBUG] stream EOF; closing")
                } else {
                    log.Printf("[DEBUG] read error: %v", err)
                }
                return
            }
            log.Printf("[DEBUG] read error after partial line: %v (processing partial)", err)
        }

        // Trim newline(s)
        line = bytes.TrimRight(line, "\r\n")
        if len(line) == 0 {
            continue // ignore empty keepalives etc.
        }

        var reqStruct models.ReqFormat
        if err := json.Unmarshal(line, &reqStruct); err != nil {
            log.Printf("[ERROR] Failed to unmarshal ReqFormat: %v (payload=%q)", err, line)
            continue
        }
    switch reqStruct.Type {
    case "register":
        log.Printf("[DEBUG] Handling register request from peer")
        // Handle registration - no need to unmarshal ReqParams for this
        continue
    case "SendMsg", "forward":
        // This is a message forwarding request
        var reqParams map[string]any
        cleanReqParams := bytes.TrimRight(reqStruct.ReqParams, "\x00")
        log.Printf("reqParams: %+v", cleanReqParams)
		if err := json.Unmarshal(cleanReqParams, &reqParams); err != nil {
			log.Printf("[ERROR] Failed to unmarshal SendMsg request: %v (payload=%q)", err, cleanReqParams)
			continue
		}
        
        log.Printf("[DEBUG] ReqParams is: %+v", reqParams)
        
        switch strings.ToUpper(fmt.Sprint(reqParams["type"])) {
        case "GET":
            log.Printf("Serving GET Req")
            resp := ServeGetReq(reqStruct.ReqParams)
            resp = bytes.TrimRight(resp, "\x00")
            resp = append(resp, '\n')
            
            log.Printf("Response for GET is: %s", string(resp))
            if _, err := s.Write(resp); err != nil {
                log.Printf("[DEBUG] Error writing resp bytes to relay stream: %v", err)
                return
            }
        case "POST":
            resp := ServePostReq(reqStruct.ReqParams, reqStruct.Body)
            resp = bytes.TrimRight(resp, "\x00")
            resp = append(resp, '\n')
            
            if _, err := s.Write(resp); err != nil {
                log.Printf("[DEBUG] Error writing resp bytes to relay stream: %v", err)
                return
            }
        default:
            log.Printf("[WARN] Unknown route type: %v", reqParams["type"])
        }
    default:
        log.Printf("[WARN] Unknown request type: %s", reqStruct.Type)
    }
    }
}

func Send(dp *models.UserPeer, ctx context.Context, targetPeerID string, reqParams json.RawMessage, body []byte) ([]byte, error) {
	//completeIP := TargetIP + ":" + targetPort

	//sends msg to relay
	var req models.ReqFormat
	req.Type = "SendMsg"
	req.PeerID = targetPeerID
	req.ReqParams = reqParams // all req data is sent in reqParams
	req.Body = body
	log.Printf("Sending req: %+v", req)

	stream, err := dp.Host.NewStream(ctx, dp.RelayID, UserPeerProtocol)
	if err != nil {
		log.Println("[DEBUG]Error opening a fetch ID stream to relay")
		return nil, err
	} else {
		log.Println("[DEBUG]Opened stream to relay successsfully")
	}

	jsonReqRelay, err := json.Marshal(req)
	if err != nil {
		log.Println("[DEBUG]Error marshalling get req to be sent to relay")
		return nil, err
		} else {
			log.Println("[DEBUG]Marshalled get req to be sent to relay successfully")
		}
	jsonReqRelay = append(jsonReqRelay, '\n')
	log.Println(jsonReqRelay)

	stream.Write(jsonReqRelay)
	log.Println("[DEBUG]Msg req sent to relay, waiting for ack")


	reader := bufio.NewReader(stream)
	log.Println("[DEBUG]Reading response from relay")
	
	var resp = make([]byte, 1024*16)
	reader.Read(resp)
	resp = bytes.TrimRight(resp, "\x00")
	defer stream.Close()

	return resp, err
}

func GetConnectedPeers(dp *models.UserPeer) []peer.ID {
	var peers []peer.ID
	for _, conn := range dp.Host.Network().Conns() {
		remotePeer := conn.RemotePeer()
		if remotePeer != dp.RelayID {
			peers = append(peers, remotePeer)
		}
	}
	log.Printf("[DEBUG] Connected peers: %v\n", peers)
	return peers
}

func Close(dp *models.UserPeer) error {
	log.Println("[DEBUG] Closing Host")
	return dp.Host.Close()
}

func XorHexToBigInt(hex1, hex2 string) *big.Int {

	bytes1, err1 := hex.DecodeString(hex1)
	bytes2, err2 := hex.DecodeString(hex2)

	if err1 != nil || err2 != nil {
		log.Fatalf("Error decoding hex: %v %v", err1, err2)
	}

	if len(bytes1) != len(bytes2) {
		log.Fatalf("Hex strings must be the same length")
	}

	xorBytes := make([]byte, len(bytes1))
	for i := 0; i < len(bytes1); i++ {
		xorBytes[i] = bytes1[i] ^ bytes2[i]
	}

	result := new(big.Int).SetBytes(xorBytes)
	return result
}
