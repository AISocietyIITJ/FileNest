package kademlia

import (
	"fmt"
	// "final/backend/pkg/helpers"
	"final/backend/pkg/types"
	"time"
)

// PING handlers - update field names
func (k *KademliaNode) HandlePing(req *types.PingRequest) (*types.PingResponse, error) {
	// if req == nil {
	// 	return nil, fmt.Errorf("ping request cannot be nil")
	// }

	// // Update routing table with sender information using PeerInfo
	// senderPeer := types.PeerInfo{
	// 	NodeID: req.SenderNodeID, // Changed from SenderID
	// 	PeerID: req.SenderPeerID, // Changed from SenderAddr
	// }
	// k.routingTable.Update(senderPeer)

	// response := &types.PingResponse{
	// 	SenderNodeID: k.NodeID, // Changed from SenderID
	// 	SenderPeerID: k.PeerID, // Changed from SenderAddr
	// 	Timestamp:    time.Now().UnixNano(),
	// 	Success:      true,
	// }

	// return response, nil
	return nil, nil
}

func (k *KademliaNode) Ping(targetNodeID []byte) (*types.PingResponse, error) {
	req := &types.PingRequest{
		SenderNodeID: k.NodeID, // Changed from SenderID
		SenderPeerID: k.PeerID, // Changed from SenderAddr
		Timestamp:    time.Now().UnixNano(),
	}

	response, err := k.network.SendPing(targetNodeID, req)
	if err != nil {
		return nil, fmt.Errorf("ping failed: %w", err)
	}

	if response.Success {
		targetPeer := types.PeerInfo{
			NodeID: response.SenderNodeID, // Changed from SenderID
			PeerID: response.SenderPeerID, // Changed from SenderAddr
		}
		k.routingTable.Update(targetPeer)
	}

	return response, nil
}

// func (k *KademliaNode) EmbeddingSearch(req *types.EmbeddingSearchRequest) (*types.EmbeddingSearchResponse, error) {
// 	if req == nil {
// 		return nil, fmt.Errorf("embedding search request cannot be nil")
// 	}

// 	// Set source information if not already set
// 	if len(req.SourceNodeID) == 0 {
// 		req.SourceNodeID = k.NodeID
// 		req.SourcePeerID = k.PeerID
// 	}

// 	response, err := k.network.SendEmbeddingSearch(req.TargetNodeID, req)
// 	if err != nil {
// 		return nil, fmt.Errorf("embedding search failed: %w", err)
// 	}

// 	// Update routing table with response source
// 	if len(response.SourceNodeID) > 0 {
// 		responseNode := types.PeerInfo{
// 			NodeID: response.SourceNodeID,
// 			PeerID: response.SourcePeerID,
// 		}
// 		k.routingTable.Update(responseNode)
// 	}

// 	return response, nil
// }

// IterativeFindNode performs iterative lookup to find and reach target NodeID
// IterativeFindNodeWithTracking performs lookup and tracks every response
// func (k *KademliaNode) IterativeFindNodeWithTracking(targetNodeID []byte) (*types.CompleteLookupResponse, error) {
// 	startTime := time.Now().UnixNano()

// 	fmt.Printf("=== Starting Tracked Iterative Lookup ===\n")
// 	fmt.Printf("Initial Node: %x\n", k.NodeID[:8])
// 	fmt.Printf("Target Node: %x\n", targetNodeID[:8])

// 	// Track the complete lookup journey
// 	lookupResponse := &types.CompleteLookupResponse{
// 		TargetNodeID: targetNodeID,
// 		LookupPath:   []types.LookupStep{},
// 		TotalHops:    0,
// 		Success:      false,
// 		StartTime:    startTime,
// 	}

// 	// Track nodes we've queried
// 	queriedNodes := make(map[string]bool)
// 	queriedNodes[string(k.NodeID)] = true

// 	// Get initial closest nodes from our routing table
// 	closestNodes := k.routingTable.FindClosest(targetNodeID, k.routingTable.K)
// 	if len(closestNodes) == 0 {
// 		return nil, fmt.Errorf("no nodes in routing table to start lookup")
// 	}

// 	// Track all discovered nodes
// 	allFoundNodes := make(map[string]types.PeerInfo)
// 	for _, peer := range closestNodes {
// 		allFoundNodes[string(peer.NodeID)] = peer
// 	}

// 	maxIterations := 10
// 	stepNumber := 1

// 	for iteration := 0; iteration < maxIterations; iteration++ {
// 		fmt.Printf("\n--- Iteration %d ---\n", iteration+1)

// 		// Check if we found the target
// 		if _, exists := allFoundNodes[string(targetNodeID)]; exists {
// 			fmt.Printf("✓ Target node found!\n")
// 			lookupResponse.Success = true
// 			break
// 		}

// 		// Select nodes to query
// 		nodesToQuery := k.selectNodesToQuery(targetNodeID, allFoundNodes, queriedNodes)
// 		if len(nodesToQuery) == 0 {
// 			fmt.Printf("No more nodes to query\n")
// 			break
// 		}

// 		// Query each node and record their responses
// 		for _, nodeToQuery := range nodesToQuery {
// 			stepStart := time.Now()
// 			fmt.Printf("Step %d: Querying node %x\n", stepNumber, nodeToQuery.NodeID[:8])

// 			// Send FIND_NODE request
// 			response, err := k.FindNode(nodeToQuery.NodeID, targetNodeID)
// 			queriedNodes[string(nodeToQuery.NodeID)] = true
// 			responseTime := time.Since(stepStart).Milliseconds()

// 			// Create lookup step record - EVERY NODE RESPONDS
// 			step := types.LookupStep{
// 				NodeID:           nodeToQuery.NodeID,
// 				PeerID:           nodeToQuery.PeerID,
// 				StepNumber:       stepNumber,
// 				NodesReturned:    []types.PeerInfo{},
// 				DistanceToTarget: k.calculateDistanceString(nodeToQuery.NodeID, targetNodeID),
// 				Timestamp:        time.Now().UnixNano(),
// 				ResponseTime:     responseTime,
// 			}

// 			if err != nil {
// 				fmt.Printf("  ❌ Node %x failed to respond: %v\n", nodeToQuery.NodeID[:8], err)
// 				step.NodesReturned = []types.PeerInfo{} // Empty response
// 			} else {
// 				fmt.Printf("  ✅ Node %x responded with %d nodes\n", nodeToQuery.NodeID[:8], len(response.ClosestNodes))
// 				step.NodesReturned = response.ClosestNodes

// 				// Add new nodes to our discovered set
// 				for _, newNode := range response.ClosestNodes {
// 					if _, exists := allFoundNodes[string(newNode.NodeID)]; !exists {
// 						allFoundNodes[string(newNode.NodeID)] = newNode
// 						fmt.Printf("    Found new node: %x\n", newNode.NodeID[:8])
// 					}
// 				}
// 			}

// 			// IMPORTANT: Record this node's response in the lookup path
// 			lookupResponse.LookupPath = append(lookupResponse.LookupPath, step)
// 			stepNumber++
// 		}
// 	}

// 	lookupResponse.TotalHops = len(lookupResponse.LookupPath)
// 	lookupResponse.EndTime = time.Now().UnixNano()

// 	fmt.Printf("\n=== Lookup Complete ===\n")
// 	fmt.Printf("Total steps: %d\n", lookupResponse.TotalHops)
// 	fmt.Printf("Success: %v\n", lookupResponse.Success)

// 	return lookupResponse, nil
// }

// // Helper to calculate distance as string for logging
// func (k *KademliaNode) calculateDistanceString(nodeID, targetID []byte) string {
// 	distance := helpers.XORDistance(nodeID, targetID)
// 	return fmt.Sprintf("%x", distance.Bytes()[:4]) // First 4 bytes as hex
// }

// Helper: Select best nodes to query next

// Helper: Get K closest nodes from discovered nodes


