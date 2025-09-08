package main

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"final/backend/pkg/identity"
	"final/backend/pkg/integration"
	"final/backend/pkg/types"
	genmodels "final/network/RelayFinal/pkg/generalpeer/models"
	"final/network/RelayFinal/pkg/generalpeer/ws"
	"final/network/RelayFinal/pkg/network"
	"final/network/RelayFinal/pkg/network/helpers"
	relayhelper "final/network/RelayFinal/pkg/relay/helpers"
	"final/network/RelayFinal/pkg/relay/models"
	"final/network/RelayFinal/pkg/relay/peer"
	"flag"
	"log"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func main() {
	// ---- Flags ----
	findval := flag.Bool("findval", false, "Search for query embedding")
	pid := flag.String("pid", "", "Target peer ID to send request to (comma sep)")
	ptype := flag.String("type", "user", "Upload a file to the network")
	nodeidHex := flag.String("nodeid", "", "Node ID for target peer (hex, comma sep)")
	store := flag.Bool("store", false, "Upload a file to the network")
	flag.Parse()

	// ---- Load Node ID ----
	SourceNodeID, err := identity.LoadOrCreateNodeID("")
	if err != nil {
		log.Println("Could not load/create the node id")
	}

	// ---- Setup ML transport ----
	mlChan := make(chan genmodels.ClusterWrapper, 10)
	mlTransport := ws.NewWebSocketTransport(":8081")
	defer mlTransport.Close()
	go func() {
		if err := mlTransport.StartMLReceiver(mlChan); err != nil {
			log.Printf("ML Receiver error: %v", err)
		}
	}()
	go func() {
		log.Println("[NET] Listening for ML messages...")
		for msg := range mlChan {
			log.Printf("[NET] Received ML message: %+v", msg)
		}
	}()

	// ---- Relay Addresses ----
	relayAddrs, err := relayhelper.GetRelayAddrFromMongo()
	if err != nil {
		log.Printf("Error during get relay addrs: %v", err.Error())
		return
	}
	log.Printf("relayAddrs in Mongo: %+v\n", relayAddrs)

	// ---- Start Peer ----
	p, err := peer.NewPeer(relayAddrs, *ptype)
	if err != nil {
		log.Printf("Error on NewDepthPeer: %v\n", err.Error())
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := peer.Start(p, ctx); err != nil {
		log.Printf("Error starting peer: %v", err)
		return
	}

	// ---- Kademlia Integration ----
	kademliaHandler := integration.NewComprehensiveKademliaHandler()
	selfNodeID, err := kademliaHandler.InitializeNode(
		p.Host.ID().String(),
		"./nodeid_embedding_map.db",
	)
	if err != nil {
		log.Printf("Failed to initialize Kademlia: %v", err)
		return
	}

	//networkHandler for dep. injection
	netHandler := network.NewNetworkHandler(kademliaHandler)
    peer.SetNetworkHandler(netHandler)
	// ---- Upsert Node and Bootstrap ----
	decSelfNodeID := hex.EncodeToString(selfNodeID)

	embed := []float64{0.1, 0.2, 0.3, 0.4, 0.5}
	if err = relayhelper.UpsertNode(decSelfNodeID, p.Host.ID().String(), embed); err != nil {
		log.Printf("Error in upserting node to mongo: %v \n", err.Error())
	} else {
		log.Println("✓ Kademlia integration initialized")
		bootstrapKademlia(kademliaHandler, p, ctx, *nodeidHex, *pid)
	}

	// ---- Print routing stats ----
	stats := kademliaHandler.GetNodeStatistics()
	routingInfo := kademliaHandler.GetRoutingInfo()
	log.Printf("📊 Node Statistics: %+v", stats)
	log.Printf("🗺️  Routing table contains %d peers", len(routingInfo))

	storeTestEmbeddings(kademliaHandler, selfNodeID)
	// ---- Handle CLI Actions ----
	if *findval {
		handleFindValueUser(p, ctx, kademliaHandler, SourceNodeID)
	}

	if *store {
		handleStoreUser(p, ctx, kademliaHandler, SourceNodeID)
	}

	// launch the HandleStoreandFindValue function as a goroutine. 

	<-ctx.Done()

	// ---- Save routing table before exit ----
	if err := saveRoutingTableToDB(kademliaHandler); err != nil {
		log.Printf("Failed to save routing table: %v", err)
	} else {
		log.Println("✓ Routing table saved to DB.")
	}
}

//
// ---- Action Handlers ----
//

func bootstrapKademlia(handler *integration.ComprehensiveKademliaHandler, p *models.UserPeer, ctx context.Context, nodeidHex, pid string) {
	if nodeidHex == "" || pid == "" {
		log.Println("No bootstrap nodes provided, skipping bootstrap")
		return
	}

	bootstrapNodes, err := helpers.ParseBootstrapFlags(nodeidHex, pid)
	if err != nil {
		log.Printf("Error parsing bootstrap flags: %v", err)
		return
	}

	if len(bootstrapNodes) > 0 {
		log.Println("🚀 Starting Kademlia bootstrap process...")
		successCount := 0
		for _, addr := range bootstrapNodes {
			nodeBytes, err := hex.DecodeString(addr.NodeID)
			if err != nil {
				log.Printf("❌ Invalid NodeID for %s: %v", addr, err)
				continue
			}
			if err := addPeerToRoutingTable(handler, addr.PeerID, nodeBytes); err != nil {
				log.Printf("❌ Bootstrap failed for %s: %+v", addr, err)
			} else {
				successCount++
				log.Printf("✅ Bootstrap successful for %s", addr)
			}
		}

		if successCount > 0 {
			log.Printf("✓ Bootstrap completed with %d/%d successful connections", successCount, len(bootstrapNodes))
			// Ping bootstrap peers
			for _, addr := range bootstrapNodes {
				if err := pingPeer(p, ctx, addr.PeerID); err != nil {
					log.Printf("pingPeer err: %v", err.Error())
				}
			}
		} else {
			log.Println("⚠️  Bootstrap failed for all nodes - running in isolated mode")
		}
	}
}

func handleFindValueUser(p *models.UserPeer, ctx context.Context, kademliaHandler *integration.ComprehensiveKademliaHandler, SourceNodeID []byte) {
    log.Println("🔍 Starting find_value process...")
    test_embedding := []float64{0.1, 0.2, 0.3, 0.4, 0.5}
    threshold := 0.7
    limit := 1

    // 1. Find the node ID of the peer storing the most similar embedding in our local DB
    targetNodeIDs, err := kademliaHandler.Node().FindSimilar(test_embedding, threshold, limit)
    if err != nil {
        log.Printf("Error finding similar node: %v", err)
        return
    }
    if len(targetNodeIDs) == 0 {
        log.Println("Could not find any target node ID above the similarity threshold.")
        return
    }
    TargetNodeID := targetNodeIDs[0].Key
    log.Printf("Found target NodeID: %s", hex.EncodeToString(TargetNodeID))

    // 2. Find the closest peer in our routing table to the target node ID
    nextNode := kademliaHandler.Node().RoutingTable().FindClosest(TargetNodeID, limit)
    if len(nextNode) == 0 {
        log.Println("Could not find a peer in the routing table to forward the request to.")
        return
    }
    nextPID := nextNode[0].PeerID
    log.Printf("Found next hop PeerID: %s", nextPID)

    // 3. Build and send the request
    params := models.EmbeddingSearchRequest{
        Type:           "GET",
        Route:          "find_value",
        SourceNodeID:   SourceNodeID,
        SourcePeerID:   p.Host.ID().String(),
        TargetNodeID:   TargetNodeID,
        ReceiverPeerID: nextPID,
        QueryEmbed:      test_embedding,
    }

    found := false
    for depth := 0; depth < 4 && !found; depth++ {
        log.Printf("Find attempt %d: sending to PeerID %s", depth+1, nextPID)

        // The body for a find_value request can be empty
        resp, err := helpers.SendJSON(p, ctx, nextPID, params, nil)
        if err != nil {
            log.Printf("Error sending find_value request: %v", err)
            break
        }

        var respDec map[string]any
        if err := json.Unmarshal(resp, &respDec); err != nil {
            log.Printf("Error unmarshalling find_value response: %v", err)
            break
        }
        log.Printf("Response (iteration %d): %+v", depth+1, respDec)

        if wasFound, ok := respDec["Found"].(bool); ok && wasFound {
            log.Printf("✅ Successfully found value: %v", respDec["Value"])
            found = true
            break
        }

        if next, ok := respDec["NextPeerID"].(string); ok && next != "" {
            log.Printf("Not the target, next hop is %s", next)
            nextPID = next
            params.ReceiverPeerID = next
        } else {
            log.Println("Search failed and no next peer was provided. Aborting.")
            break
        }
    }
    if !found {
        log.Println("⚠️ Find process finished without finding the value.")
    }
}

// func handleFindValueUser(p *models.UserPeer, ctx context.Context, kademliaHandler *integration.ComprehensiveKademliaHandler, SourceNodeID []byte) {
// 	log.Println("🔍 Starting find_value process...")
// 	test_embedding := []float64{0.1, 0.2, 0.3, 0.4, 0.5}
// 	threshold := 0.7
// 	limit := 1

// 	// 1. Find the node ID of the peer storing the most similar embedding in our local DB
// 	targetNodeIDs, err := kademliaHandler.Node().FindSimilar(test_embedding, threshold, limit)
// 	if err != nil {
// 		log.Printf("Error finding similar node: %v", err)
// 		return
// 	}
// 	if len(targetNodeIDs) == 0 {
// 		log.Println("Could not find any target node ID above the similarity threshold.")
// 		return
// 	}
// 	TargetNodeID := targetNodeIDs[0].Key
// 	log.Printf("Found target NodeID: %s", hex.EncodeToString(TargetNodeID))

// 	// 2. Find the closest peer in our routing table to the target node ID
// 	nextNode := kademliaHandler.Node().RoutingTable().FindClosest(TargetNodeID, limit)
// 	if len(nextNode) == 0 {
// 		log.Println("Could not find a peer in the routing table to forward the request to.")
// 		return
// 	}
// 	nextPID := nextNode[0].PeerID
// 	log.Printf("Found next hop PeerID: %s", nextPID)

// 	// 3. Build and send the request
// 	params := models.EmbeddingSearchRequest{
// 		Type:           "GET",
// 		Route:          "find_value",
// 		SourceNodeID:   SourceNodeID,
// 		SourcePeerID:   p.Host.ID().String(),
// 		TargetNodeID:   TargetNodeID,
// 		ReceiverPeerID: nextPID,
// 	}
// 	// while depth!=4 && found!=true keep getting responses

// 	// LOAD THE ID AND COMPARE WITH THE SOURCEID TO SEE WHAT STRUCT YOU HAVE TO BUILD
// 	depth := 1
// 	found := false
// 	for depth <= 4{
// 		// Send the request again (simulate getting a new response)
// 		requestBody, newRoute := helpers.BuildKademliaFindValueRequest(kademliaHandler, SourceNodeID, test_embedding, depth)
// 		params.Route = newRoute
// 		if found {
// 			if outputMsg, ok := requestBody["output_message"].(map[string]interface{}); ok {
// 				outputMsg["Found"] = true
// 			}
// 		}
// 		resp, err := helpers.SendJSON(p, ctx, nextPID, params, requestBody)
// 		if err != nil {
// 			log.Printf("Error sending find_value request: %v", err)
// 			break
// 		} else if len(resp) > 0 {
// 			var respDec map[string]interface{}
// 			json.Unmarshal(resp, &respDec)
// 			log.Printf("Response (depth %d): %+v", depth, respDec)
// 			// Check if 'found' is true in the response (customize as needed)
// 			if val, ok := respDec["Found"].(bool); ok && val {
// 				found = true
// 				depth++;
// 			}else{
// 				found = false
// 			}
// 			params.ReceiverPeerID = respDec["NextPeerID"].(string)
// 		} else {
// 			log.Println("Received empty response")
// 			}		
// 		if depth >=4 && found{
// 			break
// 		}
// 	}
// }

func handleStoreUser(p *models.UserPeer, ctx context.Context, kademliaHandler *integration.ComprehensiveKademliaHandler, SourceNodeID []byte) {
	log.Println("🔍 Starting store process...")
	test_embedding := []float64{0.1, 0.2, 0.3, 0.4, 0.5}
	threshold := 0.7
	limit := 1

	// 1. Find the node ID of the peer storing the most similar embedding in our local DB
	targetNodeIDs, err := kademliaHandler.Node().FindSimilar(test_embedding, threshold, limit)
	if err != nil {
		log.Printf("Error finding similar node: %v", err)
		return
	}
	if len(targetNodeIDs) == 0 {
		log.Println("Could not find any target node ID above the similarity threshold.")
		return
	}
	TargetNodeID := targetNodeIDs[0].Key
	log.Printf("Found target NodeID: %s", hex.EncodeToString(TargetNodeID))

	// 2. Find the closest peer in our routing table to the target node ID
	nextNode := kademliaHandler.Node().RoutingTable().FindClosest(TargetNodeID, limit)
	if len(nextNode) == 0 {
		log.Println("Could not find a peer in the routing table to forward the request to.")
		return
	}
	nextPID := nextNode[0].PeerID
	log.Printf("Found next hop PeerID: %s", nextPID)

	// 3. Build and send the request
	params := models.EmbeddingSearchRequest{
		Type:           "POST",
		Route:          "store",
		SourceNodeID:   SourceNodeID,
		SourcePeerID:   p.Host.ID().String(),
		TargetNodeID:   TargetNodeID,
		ReceiverPeerID: nextPID,
		QueryEmbed: test_embedding,
	}

	depth := 1
	found := false
	for depth <= 4{
		// Send the request again (simulate getting a new response)
		requestBody, newRoute := helpers.BuildKademliaStoreRequest(kademliaHandler, SourceNodeID, test_embedding, depth)
		params.Route = newRoute
		if found {
			if outputMsg, ok := requestBody["output_message"].(map[string]interface{}); ok {
				outputMsg["Found"] = true
			}
		}

		resp, err := helpers.SendJSON(p, ctx, nextPID, params, requestBody)
		if err != nil {
			log.Printf("Error sending find_value request: %v", err)
			break
		} else if len(resp) > 0 {
			var respDec map[string]interface{}
			json.Unmarshal(resp, &respDec)
			log.Printf("Response (depth %d): %+v", depth, respDec)

			// Check if 'found' is true in the response (customize as needed)
			if val, ok := respDec["Found"].(bool); ok && val {
				found = true
				depth++;
			}else {
				found = false
			}
			params.ReceiverPeerID = respDec["NextPeerID"].(string)
			nextPID = params.ReceiverPeerID
		} else {
			log.Println("Received empty response")
			}	
			
			
		if depth >=4 && found{
			break
		}
	}
	// while depth!=4 && found!=true keep getting responses
	// we also have to ensure that the response being generated is being stored at each depth

}

// ---- Helpers ----

func addPeerToRoutingTable(handler *integration.ComprehensiveKademliaHandler, pid string, nodeID []byte) error {
	log.Printf("Adding peer to routing table: PID=%s NID len=%v", pid, len(nodeID))
	return handler.AddPeerToRoutingTable(types.PeerInfo{NodeID: nodeID, PeerID: pid})
}

func pingPeer(p *models.UserPeer, ctx context.Context, pid string) error {
	params := models.PingRequest{
		Type:           "GET",
		Route:          "ping",
		ReceiverPeerID: pid,
		Timestamp:      time.Now().Unix(),
	}
	resp, err := helpers.SendJSON(p, ctx, pid, params, map[string]interface{}{})
	if err != nil {
		log.Printf("Ping failed for peer %s: %v", pid, err.Error())
		return err
	}
	log.Printf("Ping response from peer %s: %s", pid, string(resp))
	return nil
}

func saveRoutingTableToDB(handler *integration.ComprehensiveKademliaHandler) error {
	routingDBPath := "routing_table.db"
	db, err := sql.Open("sqlite3", routingDBPath)
	if err != nil {
		return err
	}
	defer db.Close()

	stmt, err := db.Prepare(`INSERT OR REPLACE INTO routing_table (node_id, peer_id) VALUES (?, ?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	routingInfo := handler.GetRoutingInfo()
	for _, peer := range routingInfo {
		nodeIDHex := hex.EncodeToString(peer.NodeID)
		_, err := stmt.Exec(nodeIDHex, peer.PeerID)
		if err != nil {
			return err
		}
	}
	return nil
}

func storeTestEmbeddings(handler *integration.ComprehensiveKademliaHandler, nodeidBytes []byte) {
	testFiles := []genmodels.ClusterFile{
		{
			Filename: "documentYug.pdf",
			Metadata: genmodels.FileMetadata{
				Name:         "documentYug.pdf",
				CreatedAt:    time.Now().Format(time.RFC3339),
				LastModified: time.Now().Format(time.RFC3339),
				FileSize:     1024.5,
				UpdatedAt:    time.Now().Format(time.RFC3339),
			},
			Embedding: []float64{0.1, 0.2, 0.3, 0.4, 0.5},
		},
		{
			Filename: "imageYug.jpg",
			Metadata: genmodels.FileMetadata{
				Name:         "imageYug.jpg",
				CreatedAt:    time.Now().Format(time.RFC3339),
				LastModified: time.Now().Format(time.RFC3339),
				FileSize:     2048.7,
				UpdatedAt:    time.Now().Format(time.RFC3339),
			},
			Embedding: []float64{0.9, 0.1, 0.0, 0.0, 0.0},
		},
	}
	for _, file := range testFiles {
		if err := handler.StoreEmbedding(nodeidBytes, file.Embedding); err != nil {
			log.Printf("Failed to store embedding for %s: %v", file.Filename, err)
		} else {
			log.Printf("✓ Stored embedding for file: %s", file.Filename)
		}
	}
}