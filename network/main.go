package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
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

type PeerDoc struct {
	PeerID    string    `bson:"peerid"`
	NodeID    string    `bson:"nodeid"`
	D1TV      []float64 `bson:"D1TV"`
	UpdatedAt time.Time `bson:"updatedAt"`
}

func main() {
	// ---- Flags ----
	findval := flag.Bool("findval", false, "Search for query embedding")
	ptype := flag.String("type", "user", "Upload a file to the network")
	store := flag.Bool("store", false, "Upload a file to the network")
	flag.Parse()

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
		"./d1tv.db",
	)
	if err != nil {
		log.Printf("Failed to initialize Kademlia Node: %v", err)
		return
	}

	//networkHandler for dep. injection
	netHandler := network.NewNetworkHandler(kademliaHandler)
	peer.SetNetworkHandler(netHandler)
	
	decSelfNodeID := hex.EncodeToString(selfNodeID)

	peers, err := relayhelper.GetAllPeersFromMongo()
	if err != nil {
		log.Printf("Error fetching peers: %v", err)
	}
	addToD1TV(peers, selfNodeID, kademliaHandler)

	// Only depth peer goes to mongo
	if(*ptype == "depth"){
		embed := []float64{0.1,0.2,0.3,0.5,1}
		if err = relayhelper.UpsertNode(decSelfNodeID, p.Host.ID().String(), embed); err != nil {
			log.Printf("Error in upserting node to mongo: %v \n", err.Error())
		} else {
			log.Println("✓ Kademlia integration initialized")
			bootstrapKademlia(kademliaHandler, peers)
		}
	} else {
		log.Println("✓ Kademlia integration initialized")
		bootstrapKademlia(kademliaHandler, peers)
	}

	// ---- Print routing stats ----
	stats := kademliaHandler.GetNodeStatistics()
	routingInfo := kademliaHandler.GetRoutingInfo()
	log.Printf("📊 Node Statistics: %+v", stats)
	log.Printf("🗺️  Routing table contains %d peers", len(routingInfo))

	// ---- Handle CLI Actions ----
	if *findval {
		handleFindValueUser(p, ctx, kademliaHandler)
	}

	if *store {
		handleStoreUser(p, ctx, kademliaHandler)
	}

	<-ctx.Done()

	// ---- Save routing table before exit ----
	if err := saveRoutingTableToDB(kademliaHandler); err != nil {
		log.Printf("Failed to save routing table: %v", err)
	} else {
		log.Println("✓ Routing table saved to DB.")
	}
}

func addToD1TV(peers []relayhelper.PeerDoc, selfNodeID []byte, kademliaHandler *integration.ComprehensiveKademliaHandler){
	//gets all D1 peers on mongo

	for _, doc := range peers {
		nodeIDStr := doc.NodeID
		peerID := doc.PeerID
		embedding := doc.D1TV
		if nodeIDStr != "" && embedding != nil {
			bootstrapID, err := hex.DecodeString(nodeIDStr)
			if(err != nil){
				log.Printf("Error while decoding string: %+v", err.Error())
				continue
			}

			//dont add self to D1TV.db
			if bytes.Equal(bootstrapID, selfNodeID) {
				continue
			}

			log.Printf("Storing embed: %v", embedding)
			kademliaHandler.StoreEmbedding(bootstrapID, peerID, embedding) // Stores to D1TV.db
		}
	}
}

func bootstrapKademlia(kademliaHandler *integration.ComprehensiveKademliaHandler, peers []relayhelper.PeerDoc) {
	for _, peer := range peers {
		nodeIDStr := peer.NodeID
		peerID := peer.PeerID
		log.Printf("Bootstrapped pid: %v \n", peerID)
		if nodeIDStr != "" && peerID != "" {
			bootstrapID, err := hex.DecodeString(nodeIDStr)
			if err == nil {
				pInfo := types.PeerInfo{NodeID: bootstrapID, PeerID: peerID}
				kademliaHandler.AddPeerToRoutingTable(pInfo)
			}
		}
	}
}

func handleFindValueUser(p *models.UserPeer, ctx context.Context, kademliaHandler *integration.ComprehensiveKademliaHandler) {
    log.Println("🔍 Starting store process...")
    test_embedding := []float64{0.15, 0.25, 0.35, 0.45, 0.55}
    threshold := 0.
	
    // Find Depth 1 nodes
    targets, err := kademliaHandler.Node().FindSimilar(test_embedding, threshold, 1)
    if err != nil {
		log.Printf("Error finding a representative node ID: %v", err)
        return
    }
    if len(targets) == 0 {
		log.Println("Could not find any bootstrap node ID to begin the store process.")
        return
    }

	//send req to all suitable peers one by one (Change to goroutine later)
    for _, target := range targets {
        targetNodeID := hex.EncodeToString(target.NodeID)
        log.Printf("Attempting to store via target: %s", targetNodeID)

        // Get the peer ID for the target node
        currentPeerInfo, errStr := handleFindNode(p, ctx, kademliaHandler, targetNodeID)
        if errStr != "" {
            log.Printf("Error finding initial target peer: %s", errStr)
            continue 
        }
        log.Printf("Found initial target peer: %+v", currentPeerInfo)


        currentNodeID := targetNodeID
        depth := 1
        maxDepth := 4
        stored := false

       	for depth <= maxDepth {
			log.Printf("Store attempt at depth %d for NodeID: %s", depth, currentNodeID)

            // Build request for current target
            params := models.EmbeddingSearchRequest{
                Type:           "POST",
                Route:          "store",
                SourceNodeID:   hex.EncodeToString(kademliaHandler.Node().NodeID),
                SourcePeerID:   kademliaHandler.Node().PeerID,
                TargetNodeID:   currentNodeID,
                ReceiverPeerID: currentPeerInfo.PeerID,
                QueryEmbed:     test_embedding,
                Depth:          depth,
                Found:          false,
            }
            resp, err := helpers.SendJSON(p, ctx, currentPeerInfo.PeerID, params, nil)
            if err != nil {
                log.Printf("Error sending JSON to peer %s: %v", currentPeerInfo.PeerID, err)
                break
            }


            var respDec models.EmbeddingStoreResponse
            if err := json.Unmarshal(resp, &respDec); err != nil {
                log.Printf("Error unmarshalling store response: %v", err)
                break
            }

            log.Printf("Store response at depth %d: Found=%t, Pruned=%t, NextNodeID=%s", 
                depth, respDec.Found, respDec.Pruned, respDec.NextNodeID)
		
            // Check if we've reached max depth (successful store)
            if depth == maxDepth {
                log.Printf("Successfully stored embedding at depth %d", depth)
                stored = true
                break
            }
			//Pruned case, try next peer
            if respDec.Pruned {
                log.Printf("Search pruned at depth %d", depth)
                break 
            }

            // Check if we found a suitable cluster and got next node
            if respDec.Found && respDec.NextNodeID != "" {
                log.Printf("Found suitable cluster, moving to next depth with NodeID: %s", respDec.NextNodeID)
                
                // Find peer info for the next node
                nextPeerInfo, errStr := handleFindNode(p, ctx, kademliaHandler, respDec.NextNodeID)
                if errStr != "" {
                    log.Printf("Error finding next peer for NodeID %s: %s", respDec.NextNodeID, errStr)
                    break // Try next target
                }

                // Update for next iteration
                currentNodeID = respDec.NextNodeID
                currentPeerInfo = nextPeerInfo
                depth = respDec.Depth
            } else {
                log.Printf("No next node provided or not found at depth %d", depth)
                break // Try next target
            }	
		
		}

		if stored{
            log.Println("✅ Store process completed successfully")
            return // Success, no need to try other targets
        }

	}
}


func handleStoreUser(p *models.UserPeer, ctx context.Context, kademliaHandler *integration.ComprehensiveKademliaHandler) {
    log.Println("🔍 Starting store process...")
    test_embedding := []float64{0.15, 0.25, 0.35, 0.45, 0.55}
	test_filepath := "home/ma/chudao/kys"
    threshold := 0.4
	
    // Find Depth 1 nodes
    targets, err := kademliaHandler.Node().FindSimilar(test_embedding, threshold, 10)
    if err != nil {
		log.Printf("Error finding a representative node ID: %v", err)
        return
    }
    if len(targets) == 0 {
		log.Println("Could not find any bootstrap node ID to begin the store process.")
        return
    }

	//send req to all suitable peers one by one (Change to goroutine later)
    for _, target := range targets {
        targetNodeID := hex.EncodeToString(target.NodeID)
        log.Printf("Attempting to store via target: %s", targetNodeID)

        // Get the peer ID for the target node
        currentPeerInfo, errStr := handleFindNode(p, ctx, kademliaHandler, targetNodeID)
        if errStr != "" {
            log.Printf("Error finding initial target peer: %s", errStr)
            continue 
        }
        log.Printf("Found initial target peer: %+v", currentPeerInfo)

        currentNodeID := targetNodeID
        depth := 1
        maxDepth := 4
        stored := false

       	for depth <= maxDepth {
			log.Printf("Store attempt at depth %d for NodeID: %s", depth, currentNodeID)

            // Build request for current target
            params := models.EmbeddingStoreRequest{
                Type:           "POST",
                Route:          "store",
                SourceNodeID:   hex.EncodeToString(kademliaHandler.Node().NodeID),
                SourcePeerID:   kademliaHandler.Node().PeerID,
                TargetNodeID:   currentNodeID,
                ReceiverPeerID: currentPeerInfo.PeerID,
				FilePath: test_filepath,
                QueryEmbed:     test_embedding,
                Depth:          depth,
                Found:          false,
            }
            resp, err := helpers.SendJSON(p, ctx, currentPeerInfo.PeerID, params, nil)
            if err != nil {
                log.Printf("Error sending JSON to peer %s: %v", currentPeerInfo.PeerID, err)
                break
            }


            var respDec models.EmbeddingStoreResponse
            if err := json.Unmarshal(resp, &respDec); err != nil {
                log.Printf("Error unmarshalling store response: %v", err)
                break
            }

            log.Printf("Store response at depth %d: Found=%t, Pruned=%t, NextNodeID=%s", 
                depth, respDec.Found, respDec.Pruned, respDec.NextNodeID)
		
            // Check if we've reached max depth (successful store)
            if depth == maxDepth {
                log.Printf("Successfully stored embedding at depth %d", depth)
                stored = true
                break
            }
			//Pruned case, try next peer
            if respDec.Pruned {
                log.Printf("Search pruned at depth %d", depth)
                break 
            }

            // Check if we found a suitable cluster and got next node
            if respDec.Found && respDec.NextNodeID != "" {
                log.Printf("Found suitable cluster, moving to next depth with NodeID: %s", respDec.NextNodeID)
                
                // Find peer info for the next node
                nextPeerInfo, errStr := handleFindNode(p, ctx, kademliaHandler, respDec.NextNodeID)
                if errStr != "" {
                    log.Printf("Error finding next peer for NodeID %s: %s", respDec.NextNodeID, errStr)
                    break // Try next target
                }

                // Update for next iteration
                currentNodeID = respDec.NextNodeID
                currentPeerInfo = nextPeerInfo
                depth = respDec.Depth
            } else {
                log.Printf("No next node provided or not found at depth %d", depth)
                break // Try next target
            }	
		
		}

		if stored{
            log.Println("✅ Store process completed successfully")
            return // Success, no need to try other targets
        }
		
	}
}

func handleFindNode(p *models.UserPeer, ctx context.Context, kademliaHandler *integration.ComprehensiveKademliaHandler, TargetNodeID string) (types.PeerInfo, string){
	decTargetNodeID, _ := hex.DecodeString(TargetNodeID)
    
	contacted := make(map[string]bool)
    maxIterations := 10 // Prevent infinite loops

    for iteration := range maxIterations {
		log.Printf("Find_node iteration %d", iteration+1)	
		
		// Check own RT
		closestNodes := kademliaHandler.Node().RoutingTable().FindClosest(decTargetNodeID, 3)
		if(len(closestNodes) == 0){
			log.Println("Could not find any node ID to begin the store process.")
			return types.PeerInfo{}, "Could not find any node ID to begin the store process."
		}

        // Find a peer we haven't contacted yet
        var nextPeer *types.PeerInfo
        for _, peer := range closestNodes {
            if !contacted[peer.PeerID] {
                nextPeer = &peer
                break
            }
        }
        if nextPeer == nil {
            log.Println("No more uncontacted peers to query")
            break
        }
		// mark current peer as contacted
		contacted[nextPeer.PeerID] = true
		
		log.Printf("Querying peer %s for target %s", hex.EncodeToString(nextPeer.NodeID), TargetNodeID)
		params := models.FindNodeRequest{
			Type: "GET",
			Route: "find_node",
			SenderNodeID: hex.EncodeToString(kademliaHandler.Node().NodeID),
			SenderPeerID: kademliaHandler.Node().PeerID,
			ReceiverNodeID: hex.EncodeToString(nextPeer.NodeID),
			TargetNodeID: TargetNodeID,
			Timestamp: time.Now().Unix(),
		}
		resp, err := helpers.SendJSON(p, ctx, nextPeer.PeerID, params, nil)
		if(err != nil){
			log.Printf("Error sending JSON to peer : %+v", err.Error())
			continue
		}

		var respDec models.FindNodeResponse
        if err := json.Unmarshal(resp, &respDec); err != nil {
            log.Printf("Error unmarshalling find_node response: %v", err)
            continue
        }

        if len(respDec.ClosestNodes) == 0 {
            log.Println("No closer nodes found, search complete")
            break
        }
		log.Printf("Received response with %d closest nodes", len(respDec.ClosestNodes))

		if(respDec.Found){
			return respDec.ClosestNodes[0], ""
		}
		// new nodes found, add peers to own RT. This helps in FindClosest we do above.
		foundCloser := false
		for _, peerInfo := range respDec.ClosestNodes{
			kademliaHandler.AddPeerToRoutingTable(peerInfo)

			if(!contacted[peerInfo.PeerID]){
				foundCloser = true
			}
			if(!foundCloser){
				log.Printf("No closer nodes found. Stopping FindNode midway.")
				break
			}
		}
	}

	return types.PeerInfo{}, "Failed to find the target node"
}

// func handleStoreUser(p *models.UserPeer, ctx context.Context, kademliaHandler *integration.ComprehensiveKademliaHandler, SourceNodeID []byte) {
// 	log.Println("🔍 Starting store process...")
// 	test_embedding := []float64{0.15, 0.25, 0.35, 0.45, 0.55}
// 	threshold := 0.
// 	limit := 1
// 	maxDepth := 1 // will keep it as 1 for now. will change it to 4 in the future// Define max iterations
	
// 	SourceNodeIDEnc := hex.EncodeToString(SourceNodeID)
	
// 	params := models.EmbeddingSearchRequest{
// 		Type:           "POST",
// 		Route:          "store",
// 		SourceNodeID:   SourceNodeIDEnc,
// 		SourcePeerID:   p.Host.ID().String(),
// 		// TargetNodeID:   TargetNodeIDEnc,
// 		// ReceiverPeerID: nextPID,
// 		QueryEmbed:     test_embedding,
// 		Found: false,
// 	}

// 	// 1. Find the node ID of the bootstrap peer storing the most similar embedding. only when found == true
// 	targetNodeIDs, err := kademliaHandler.Node().FindSimilar(test_embedding, threshold, limit)
// 	if err != nil {
// 		log.Printf("Error finding similar node: %v", err)
// 		return
// 	}
// 	if len(targetNodeIDs) == 0 {
// 		log.Println("Could not find any target node ID above the similarity threshold.")
// 		return
// 	}
// 	//this is bootstrap NID only
// 	TargetNodeID := targetNodeIDs[0].Key
// 	TargetNodeIDEnc := hex.EncodeToString(TargetNodeID)
// 	params.TargetNodeID = TargetNodeIDEnc
	
	
// 	// 2. Find the closest peer in our routing table to the target node ID
// 	nextNode := kademliaHandler.Node().RoutingTable().FindClosest(TargetNodeID, limit)
// 	if len(nextNode) == 0 {
// 		log.Println("Could not find a peer in the routing table to forward the request to.")
// 		return
// 	}
// 	nextPID := nextNode[0].PeerID
// 	log.Printf("Found initial next hop PeerID: %s", nextPID)
	
// 	// 3. Build the initial request
// 	found := false
// 	depth := 1
// 	for(depth <= maxDepth){
// 		if(depth == 4){
// 			log.Printf("Found target node. storing embed\n")
// 			// !!! Store embed here
// 			break
// 		}

// 		// Build the request body for the current iteration
// 		requestBody, newRoute := helpers.BuildKademliaStoreRequest(kademliaHandler, SourceNodeID, test_embedding, depth)
// 		params.Route = newRoute
		
// 		resp, err := helpers.SendJSON(p, ctx, nextPID, params, requestBody)
// 		if err != nil {
// 			log.Printf("Error sending store request: %v", err)
// 			break
// 		}
// 		if len(resp) == 0 {
// 			log.Println("Received empty response, aborting.")
// 			break
// 		}
		
// 		var respDec map[string]interface{}
// 		if err := json.Unmarshal(resp, &respDec); err != nil {
// 			log.Printf("Error unmarshalling response: %v", err)
// 			break
// 		}
// 		log.Printf("Response (iteration %d): %+v", depth, respDec)
		

// 		if (respDec["found"].(bool)){
// 			params.TargetNodeID = respDec["NextNodeID"].(string)
// 			depth++
// 		}

// 		closestNodes := kademliaHandler.Node().RoutingTable().FindClosest(TargetNodeID, limit)
// 		if len(closestNodes) == 0 {
// 			log.Println("Could not find a peer in the routing table to forward the request to.")
// 			return
// 		}

// 		closestNode := closestNodes[0]
// 		params.NextPeerID = closestNode.PeerID
// 		params.NextNodeID = string(closestNode.NodeID)
		
// 		// if wasFound, ok := respDec["Found"].(bool); ok && wasFound {
// 		// 	log.Println("✅ Store successful, value has been stored.")
// 		// 	found = true
// 		// 	break // Exit the loop since we are done
// 		// }
		
// 		// // If not found, get the next peer to contact
// 		// if next, ok := respDec["NextPeerID"].(string); ok && next != "" {
// 		// 	log.Printf("Not the final target, next hop is %s", next)
// 		// 	nextPID = next
// 		// 	params.ReceiverPeerID = next // Update for the next iteration
// 		// 	} else {
// 		// 	log.Println("Store process did not complete and no next peer was provided. Aborting.")
// 		// 	break
// 		// }
// 	}

// 	if !found {
// 		log.Println("⚠️ Store process finished without confirmation.")
// 	}
// }
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

// func storeTestEmbeddings(handler *integration.ComprehensiveKademliaHandler, nodeidBytes []byte) {
// 	testFiles := []genmodels.ClusterFile{
// 		{
// 			Filename: "documentYug.pdf",
// 			Metadata: genmodels.FileMetadata{
// 				Name:         "documentYug.pdf",
// 				CreatedAt:    time.Now().Format(time.RFC3339),
// 				LastModified: time.Now().Format(time.RFC3339),
// 				FileSize:     1024.5,
// 				UpdatedAt:    time.Now().Format(time.RFC3339),
// 			},
// 			Embedding: []float64{0.1, 0.2, 0.3, 0.4, 0.5},
// 		},
// 		{
// 			Filename: "imageYug.jpg",
// 			Metadata: genmodels.FileMetadata{
// 				Name:         "imageYug.jpg",
// 				CreatedAt:    time.Now().Format(time.RFC3339),
// 				LastModified: time.Now().Format(time.RFC3339),
// 				FileSize:     2048.7,
// 				UpdatedAt:    time.Now().Format(time.RFC3339),
// 			},
// 			Embedding: []float64{0.9, 0.1, 0.0, 0.0, 0.0},
// 		},
// 	}
// 	for _, file := range testFiles {
// 		if err := handler.StoreEmbedding(nodeidBytes, file.Embedding); err != nil {
// 			log.Printf("Failed to store embedding for %s: %v", file.Filename, err)
// 		} else {
// 			log.Printf("✓ Stored embedding for file: %s", file.Filename)
// 		}
// 	}
// }
