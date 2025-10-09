package main

import (
	"crypto/rand"
	"crypto/sha256"
	"io"
	"final/network/RelayFinal/pkg/relay/helpers"

	//"io"
	"math/big"
	"sort"
	"strings"
	"sync"

	//Peers "Depthprotocol/peer"

	"context"
	//"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	relay "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"
	libp2ptls "github.com/libp2p/go-libp2p/p2p/security/tls"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	"github.com/libp2p/go-libp2p/p2p/transport/websocket"
	ma "github.com/multiformats/go-multiaddr"

	"go.mongodb.org/mongo-driver/mongo"
)

type RelayDist struct {
	relayID string
	dist    *big.Int
}

const DepthProtocol = protocol.ID("/depth/1.0.0")

//var RelayMultiAddrList = []string{"/dns4/0.tcp.in.ngrok.io/tcp/14395/p2p/12D3KooWLBVV1ty7MwJQos34jy1WqGrfkb3bMAfxUJzCgwTBQ2pn",}

type reqFormat struct {
	Type      string          `json:"type,omitempty"`
	//PubIP     string          `json:"pubip,omitempty"`
	PeerID    string			`json:"peerid"`
	ReqParams json.RawMessage `json:"reqparams,omitempty"`
	Body      json.RawMessage `json:"body,omitempty"`
}

// var (
// 	IDmap = make(map[string]string)
// 	mu    sync.RWMutex
// )

var (
	ConnectedPeers []string 
	mu sync.RWMutex
)

var RelayHost host.Host

var (
	MongoClient *mongo.Client
)

// type respFormat struct {
// 	Type string `json:"type"`
// 	Resp []byte `json:"resp"`
// }

type RelayEvents struct{}

var OwnRelayAddrFull string

//Listen and ListenClose are implemented empty to adhere to network.Notifiee interface
func (re *RelayEvents) Listen(net network.Network, addr ma.Multiaddr)      {}
func (re *RelayEvents) ListenClose(net network.Network, addr ma.Multiaddr) {}
func (re *RelayEvents) Connected(net network.Network, conn network.Conn) {
	fmt.Printf("[INFO] Peer connected: %s\n", conn.RemotePeer())
}
func (re *RelayEvents) Disconnected(net network.Network, conn network.Conn) {
	fmt.Printf("[INFO] Peer disconnected: %s\n", conn.RemotePeer())
	// Remove peer from IDmap if needed
	mu.Lock()
	if contains(ConnectedPeers,conn.RemotePeer().String()){
		remove(&ConnectedPeers, conn.RemotePeer().String())
	}
	mu.Unlock()
}

func main() {
	var err error

	fmt.Println("STARTING RELAY CODE")
	godotenv.Load(".env")

	mongo_uri := os.Getenv("MONGO_URI")
	if mongo_uri == "" {
		log.Fatal("[FATAL] MONGO_URI not set in environment")
	}
	fmt.Println("[DEBUG] Using Mongo URI:", mongo_uri)

	MongoClient, err = helpers.SetupMongo(mongo_uri)
	if err != nil {
		fmt.Printf("[DEBUG] Error connecting to MongoDB: %v\n", err.Error())
		return
	}

	fmt.Println("[DEBUG] Creating connection manager...")
	connMgr, err := connmgr.NewConnManager(100, 400)
	if err != nil {
		log.Fatalf("[ERROR] Failed to create connection manager: %v", err)
	}

	privKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		panic(err)
	}

	// --- FIX: Use $PORT instead of hardcoded 443 ---
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080" // default for local runs
	}

	fmt.Println("[DEBUG] Creating relay host on port", port)
	RelayHost, err = libp2p.New(
		libp2p.Identity(privKey),
		libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/0.0.0.0/tcp/%s/ws", port)),
		libp2p.Security(libp2ptls.ID, libp2ptls.New),
		libp2p.ConnectionManager(connMgr),
		libp2p.EnableNATService(),
		libp2p.EnableRelayService(),
		libp2p.Transport(tcp.NewTCPTransport),
		libp2p.Transport(websocket.New),
	)
	if err != nil {
		log.Fatalf("[ERROR] Failed to create relay host: %v", err)
	}
	RelayHost.Network().Notify(&RelayEvents{})

	hostName := os.Getenv("RENDER_EXTERNAL_HOSTNAME")
	if hostName == "" {
		hostName = "localhost" // fallback for local testing
	}
	OwnRelayAddrFull = fmt.Sprintf("/dns4/%s/tcp/%s/wss/p2p/%s",
		hostName, "443", RelayHost.ID().String(),
	)

	err = helpers.UpsertRelayAddr(MongoClient, OwnRelayAddrFull)
	if(err != nil){
		log.Printf("Error during upsertion: %v", err.Error())
	}

	customRelayResources := relay.Resources{
		ReservationTTL:         time.Hour,
		MaxReservations:        1000,
		MaxCircuits:            64,
		BufferSize:             64 * 1024,
		MaxReservationsPerPeer: 10,
		MaxReservationsPerIP:   400,
		MaxReservationsPerASN:  64,
	}

	fmt.Println("[DEBUG] Enabling circuit relay service...")
	_, err = relay.New(RelayHost, relay.WithResources(customRelayResources))
	if err != nil {
		log.Fatalf("[ERROR] Failed to enable relay service: %v", err)
	}

	fmt.Printf("[INFO] Relay started!\n")
	fmt.Printf("[INFO] Peer ID: %s\n", RelayHost.ID())

	for _, addr := range RelayHost.Addrs() {
		fmt.Printf("[INFO] Relay Address: %s/p2p/%s\n", addr, RelayHost.ID())
	}
	fmt.Printf("[INFO] Own Relay Addr Full: %s\n", OwnRelayAddrFull)

	// set stream handler
	RelayHost.SetStreamHandler(DepthProtocol, handleDepthStream)

	// Lists connected peers every 5 sec
	go func() {
		for {
			fmt.Println("[DEBUG] Connected peers:", ConnectedPeers)
			time.Sleep(5 * time.Second)
		}
	}()

	addr, _ := helpers.GetRelayAddrFromMongo()
	go PingTargets(addr, 5*time.Minute)

	// wait for interrupt
	fmt.Println("[DEBUG] Waiting for interrupt signal...")
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c

	fmt.Println("[INFO] Shutting down relay...")
}


func remove(Lists *[]string, val string) {
    for i, item := range *Lists {
        if item == val {
            *Lists = append((*Lists)[:i], (*Lists)[i+1:]...)
            return
        }
    }
}


func PingTargets(addresses []string, interval time.Duration) {
	go func() {
		for {
			for _, multiAddrStr := range addresses {
				// Parse the multiaddress string
				_, err := ma.NewMultiaddr(multiAddrStr)
				if err != nil {
					log.Printf("[WARN] Could not parse multiaddress %s: %v\n", multiAddrStr, err)
					continue
				}
			}
			time.Sleep(interval)
		}
	}()
}

func contains(arr []string, target string) bool {
	for _, vals := range arr {
		if vals == target {
			return true
		}
	}
	return false
}

func handleDepthStream(s network.Stream) {
	fmt.Println("[DEBUG] Incoming Depth stream from", s.Conn().RemoteMultiaddr())
	defer s.Close()
	//reader := bufio.NewReader(s)
		decoder := json.NewDecoder(s)

	for {
		var req reqFormat
		err := decoder.Decode(&req)
		if err != nil {
			// io.EOF means the other side closed the connection cleanly.
			if err != io.EOF {
				fmt.Printf("[DEBUG] Error decoding JSON at relay: %v\n", err)
			}
			return // Exit the loop on any error or clean disconnect.
		}
		fmt.Printf("Req by user is: %+v \n", req)

		//peer to relay register request
		if req.Type == "register" {
			peerID := s.Conn().RemotePeer()
			peerID2 := req.PeerID
				
			if peerID2 != peerID.String() {
				fmt.Printf("SELF PEER ID MISMATCH\nID1: %v \nID2: %v\n", peerID, peerID2)
				return
			}

			fmt.Printf("[INFO]Given peerID is %s \n", req.PeerID)
			fmt.Println("[INFO]Registering the peer into relay")
			mu.Lock()
			//IDmap[req.PubIP] = peerID.String()
			ConnectedPeers = append(ConnectedPeers, peerID.String())
			mu.Unlock()
		}

		//peer to relay sendMsg request
		if req.Type == "SendMsg" {
			mu.RLock()
			var targetPeerID string

			if contains(ConnectedPeers, req.PeerID) {
				targetPeerID = req.PeerID
			}
			mu.RUnlock()

			// checks if the target peer is connected to the relay or some other relay
			// have to handle some logic here but later

			if targetPeerID == "" {
				fmt.Println("[DEBUG]This peer is not on this relay, contacting other relay")
				targetRelayAddr := GetRelayAddr(req.PeerID)
				if targetRelayAddr == "" {
					fmt.Println("Can't get relay addr from mongoDB")
					s.Write([]byte("[DEBUG]Can't get Relay addresses from database, retry again"))
					return
				}
				if targetRelayAddr == OwnRelayAddrFull {
					s.Write([]byte("[DEBUG]Target Peer not in network"))
					return
				}
				
				//construct and send forward request to peer
				var forwardReq reqFormat
				forwardReq.Body = req.Body
				forwardReq.ReqParams = req.ReqParams
				forwardReq.PeerID = req.PeerID
				forwardReq.Type = "forward"

				relayMA, err := ma.NewMultiaddr(targetRelayAddr)
				if err != nil {
					fmt.Println("[DEBUG] Failed to parse relay multiaddr:", err)
					return
				}

				TargetRelayInfo, err := peer.AddrInfoFromP2pAddr(relayMA)
				if err != nil {
					fmt.Println("[DEBUG] Failed to parse target relay info:", err)
					return
				}

				err = RelayHost.Connect(context.Background(), *TargetRelayInfo)
				if err != nil {
					fmt.Println("[DEBUG] Failed to connect to target relay:", err)
					return
				}

				// OPENS stream to target peer
				forwardStream, err := RelayHost.NewStream(context.Background(), TargetRelayInfo.ID, DepthProtocol)
				if err != nil {
					fmt.Println("[DEBUG] Failed to open stream to target relay:", err)
					return
				}
				defer forwardStream.Close()

				// encodes fwdReq to fwdStream
				encoder := json.NewEncoder(forwardStream)
				if err := encoder.Encode(forwardReq); err != nil {
					fmt.Println("[DEBUG] Failed to write forward request to stream:", err)
					return
				}

				// Read the response back using a decoder
				var respBody json.RawMessage
				respDecoder := json.NewDecoder(forwardStream)
				if err := respDecoder.Decode(&respBody); err != nil {
					fmt.Println("[DEBUG] Error reading response from target relay:", err)
					return
				}

				log.Printf("[Debug]Forwarded Resp from relay : %s : %s \n", TargetRelayInfo.ID.String(), string(respBody))

				_, err = s.Write(respBody)
				if err != nil {
					fmt.Println("[DEBUG] Error sending back to original sender:", err)
					return
				}

			} else {
				fmt.Println("Target peer ID: ", targetPeerID)
				if RelayHost == nil {
					fmt.Println("[FATAL] RelayHost is nil!")
					return
				}

				targetID, err := peer.Decode(targetPeerID)
				if err != nil {
					log.Printf("[ERROR] Invalid Peer ID: %v", err)
					s.Write([]byte("invalid peer id"))
					return
				}

				relayBaseAddr, err := ma.NewMultiaddr("/p2p/" + RelayHost.ID().String())
				if err != nil {
					log.Fatal("relayBaseAddr error:", err)
				}
				circuitAddr, _ := ma.NewMultiaddr("/p2p-circuit")
				targetAddr, _ := ma.NewMultiaddr("/p2p/" + targetID.String())
				fullAddr := relayBaseAddr.Encapsulate(circuitAddr).Encapsulate(targetAddr)
				fmt.Println("[DEBUG] Target fullAddr: ", fullAddr.String())

				addrInfo, err := peer.AddrInfoFromP2pAddr(fullAddr)
				if err != nil {
					log.Printf("Invalid relayed multiaddr: %s", fullAddr)
					s.Write([]byte("bad relayed addr"))
					return
				}

				RelayHost.Peerstore().AddAddrs(addrInfo.ID, addrInfo.Addrs, peerstore.PermanentAddrTTL)

				err = RelayHost.Connect(context.Background(), *addrInfo)
				if err != nil {
					log.Printf("[ERROR] Failed to connect to relayed peer: %v", err)
				}

				sendStream, err := RelayHost.NewStream(context.Background(), targetID, DepthProtocol)
				if err != nil {
					fmt.Println("[DEBUG]Error opening stream to target peer", err)
					s.Write([]byte("failed"))
					return
				}
				defer sendStream.Close()

				// Use an encoder to write the JSON object to the target peer
				encoder := json.NewEncoder(sendStream)
				if err := encoder.Encode(req); err != nil {
					fmt.Println("[DEBUG]Error sending message despite stream opened:", err)
					return
				}

				// Read the response back from the target using a decoder
				var respBody json.RawMessage
				respDecoder := json.NewDecoder(sendStream)
				if err := respDecoder.Decode(&respBody); err != nil {
					fmt.Println("[DEBUG]Error reading response from target peer:", err)
					return
				}

				fmt.Printf("[Debug]Resp from %s : %s \n", targetID.String(), string(respBody))

				_, err = s.Write(respBody)
				if err != nil {
					fmt.Println("[DEBUG]Error sending response back:", err)
				}
			}
		}
		if req.Type == "forward" {
			mu.RLock()
			var targetPeerID string
			if contains(ConnectedPeers, req.PeerID) {
				targetPeerID = req.PeerID
			}
			mu.RUnlock()

			if targetPeerID == "" {
				fmt.Println("[DEBUG] Target peer not found in this relay")
				s.Write([]byte("Target peer not found"))
				return
			}

			targetID, err := peer.Decode(targetPeerID)
			if err != nil {
				fmt.Println("[DEBUG] Invalid target peer ID")
				return
			}

			relayID := RelayHost.ID()
			relayBaseAddr, _ := ma.NewMultiaddr("/p2p/" + relayID.String())
			circuitAddr, _ := ma.NewMultiaddr("/p2p-circuit")
			targetAddr, _ := ma.NewMultiaddr("/p2p/" + targetID.String())
			fullAddr := relayBaseAddr.Encapsulate(circuitAddr).Encapsulate(targetAddr)

			addrInfo, err := peer.AddrInfoFromP2pAddr(fullAddr)
			if err != nil {
				fmt.Println("[DEBUG] Invalid relayed address")
				return
			}

			RelayHost.Peerstore().AddAddrs(addrInfo.ID, addrInfo.Addrs, peerstore.PermanentAddrTTL)

			err = RelayHost.Connect(context.Background(), *addrInfo)
			if err != nil {
				fmt.Println("[DEBUG] Failed to connect to target peer at this relay")
				return
			}

			sendStream, err := RelayHost.NewStream(context.Background(), targetID, DepthProtocol)
			if err != nil {
				fmt.Println("[DEBUG] Failed to open stream to target peer")
				return
			}
			defer sendStream.Close()

			// Use an encoder to forward the request
			encoder := json.NewEncoder(sendStream)
			if err := encoder.Encode(req); err != nil {
				fmt.Println("[DEBUG]Error sending message despite stream opened:", err)
				return
			}

			// Use a decoder to read the response
			var respBody json.RawMessage
			respDecoder := json.NewDecoder(sendStream)
			if err := respDecoder.Decode(&respBody); err != nil {
				fmt.Println("[DEBUG]Error reading forwarded response from target peer:", err)
				return
			}
			fmt.Printf("[Debug]Resp from %s : %s \n", targetID.String(), string(respBody))

			_, err = s.Write(respBody)
			if err != nil {
				fmt.Println("[DEBUG]Error sending response back:", err)
			}
		}
	}
}

func GetRelayAddr(peerID string) string {
	RelayMultiAddrList, err := helpers.GetRelayAddrFromMongo()

	if err != nil {
		fmt.Println("[DEBUG]Error getting from mongo error : ",err)
		return ""
	}
	var relayList []string
	for _, multiaddr := range RelayMultiAddrList {
		if multiaddr == OwnRelayAddrFull{
			continue;
		}
		parts := strings.Split(multiaddr, "/")
		relayList = append(relayList, parts[len(parts)-1])
	}

	var distmap []RelayDist

	h1 := sha256.New() // Use sha256.New() for SHA-256
	h1.Write([]byte(peerID))
	peerIDhash := hex.EncodeToString(h1.Sum(nil))

	for _, relay := range relayList {

		h_R := sha256.New() // Use sha256.New() for SHA-256
		h_R.Write([]byte(relay))
		RelayIDhash := hex.EncodeToString(h_R.Sum(nil))

		dist := XorHexToBigInt(peerIDhash, RelayIDhash)

		distmap = append(distmap, RelayDist{dist: dist, relayID: relay})
	}

	sort.Slice(distmap, func(i, j int) bool {
		return distmap[i].dist.Cmp(distmap[j].dist) < 0
	})

	relayIDused := distmap[0].relayID

	var relayAddr string

	for _, multiaddr := range RelayMultiAddrList {
		parts := strings.Split(multiaddr, "/")
		if parts[len(parts)-1] == relayIDused {
			relayAddr = multiaddr
			break
		}
	}

	return relayAddr
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


