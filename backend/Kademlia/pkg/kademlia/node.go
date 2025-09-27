package kademlia

import (
	"crypto/sha1"
	"encoding/binary"
	"final/backend/pkg/helpers"
	"final/backend/pkg/storage"
	"final/backend/pkg/types"
	"fmt"
	"math"
	"sort"
	"time"
)

type KademliaNode struct {
	// RoutingTable returns the node's routing table (exported getter)
	NodeID       []byte            // Persistent NodeID
	PeerID       string            // Ephemeral libp2p PeerID
	routingTable *RoutingTable     // stores nodeIDs which have contacted the Node before
	storage      storage.Interface // stores nodeIDs which match the TV of this node
	network      NetworkInterface
}

func NewKademliaNode(nodeID []byte, peerID string, network NetworkInterface, dbPath string) (*KademliaNode, error) {
	// RoutingTable returns the node's routing table (exported getter)}
	// Initialize SQLite storage
	// ADD THIS VALIDATION:
	if len(nodeID) != 20 {
		return nil, fmt.Errorf("nodeID must be 20 bytes (160 bits), got %d", len(nodeID))
	}

	sqliteStorage, err := storage.NewSQLiteStorage(dbPath)
	if err != nil {
		return nil, err
	}

	return &KademliaNode{
		NodeID:       nodeID,
		PeerID:       peerID,
		routingTable: NewRoutingTable(nodeID, peerID, 20), // K=20
		storage:      sqliteStorage,                       // Initialize storage
		network:      network,
	}, nil
}

func (k *KademliaNode) RoutingTable() *RoutingTable {
	return k.routingTable
}

// Storage wrapper functions - Add these to your node.go
func (k *KademliaNode) StoreNodeEmbedding(nodeID []byte, embedding []float64) error {
	return k.storage.StoreNodeEmbedding(nodeID, embedding)
}

func (k *KademliaNode) FindSimilar(queryEmbed []float64, threshold float64, limit int) ([]storage.EmbeddingResult, error) {
	return k.storage.FindSimilar(queryEmbed, threshold, limit)
}

func (k *KademliaNode) GetID() []byte {
	return k.NodeID
}

func (k *KademliaNode) GetAddress() string {
	return k.PeerID
}

func (k *KademliaNode) CosineSimilarity(a, b []float64) (float64, error) {
	if len(a) != len(b) {
		return 0, fmt.Errorf("embedding dimensions don't match: %d != %d", len(a), len(b))
	}

	if len(a) == 0 {
		return 0, fmt.Errorf("empty embedding vectors")
	}

	var dotProduct, normA, normB float64

	// Calculate dot product and norms in one pass
	for i := range a {
		dotProduct += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}

	// Handle zero vectors
	if normA == 0 || normB == 0 {
		return 0.0, nil
	}

	// Calculate cosine similarity
	similarity := dotProduct / (math.Sqrt(normA) * math.Sqrt(normB))

	return similarity, nil
}

// PrintRoutingTable displays the node's routing table
func (k *KademliaNode) PrintRoutingTable() {
	fmt.Printf("\n=== ROUTING TABLE FOR NODE %x ===\n", k.NodeID[:8])
	k.routingTable.PrintRoutingTable()
}

// PrintRoutingTableSummary displays a compact view of the routing table
func (k *KademliaNode) PrintRoutingTableSummary() {
	fmt.Printf("\n=== SUMMARY FOR NODE %x ===\n", k.NodeID[:8])
	k.routingTable.PrintRoutingTableSummary()
}

// PrintPeerInfo displays detailed peer information
func (k *KademliaNode) PrintPeerInfo() {
	fmt.Printf("\n=== PEER INFO FOR NODE %x ===\n", k.NodeID[:8])
	k.routingTable.PrintPeerInfo()
}

// PrintBucket displays a specific bucket's contents
func (k *KademliaNode) PrintBucket(bucketIndex int) {
	fmt.Printf("\n=== BUCKET %d FOR NODE %x ===\n", bucketIndex, k.NodeID[:8])
	k.routingTable.PrintBucket(bucketIndex)
}

// FindNode sends a FIND_NODE RPC to the target node to search for searchTargetID
func (k *KademliaNode) FindNode(targetNodeID []byte, searchTargetID []byte) (*types.FindNodeResponse, error) {
	if targetNodeID == nil {
		return nil, fmt.Errorf("target node ID cannot be nil")
	}

	if searchTargetID == nil {
		return nil, fmt.Errorf("search target ID cannot be nil")
	}

	req := &types.FindNodeRequest{
		SenderNodeID: k.NodeID,
		SenderPeerID: k.PeerID,
		TargetID:     searchTargetID,
		Timestamp:    time.Now().UnixNano(),
	}

	response, err := k.network.SendFindNode(targetNodeID, req)
	if err != nil {
		return nil, fmt.Errorf("find node failed: %w", err)
	}

	// Update routing table with nodes from response
	for _, peer := range response.ClosestNodes {
		k.routingTable.Update(peer)
	}

	return response, nil
}

// HandleFindNode processes an incoming FIND_NODE request from another node
func (k *KademliaNode) HandleFindNode(req *types.FindNodeRequest) (*types.FindNodeResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("find node request cannot be nil")
	}

	// Update routing table with sender
	if len(req.SenderNodeID) > 0 {
		senderPeer := types.PeerInfo{
			NodeID: req.SenderNodeID,
			PeerID: req.SenderPeerID,
		}
		k.routingTable.Update(senderPeer)
	}

	// Find K closest nodes to the target using XOR distance
	closestNodes := k.routingTable.FindClosest(req.TargetID, k.routingTable.K)

	response := &types.FindNodeResponse{
		SenderNodeID: k.NodeID,
		SenderPeerID: k.PeerID,
		ClosestNodes: closestNodes,
		Timestamp:    time.Now().UnixNano(),
		Success:      true,
	}

	return response, nil
}

// findNextPeerForSearch finds the best next peer for embedding search routing
func (k *KademliaNode) findNextPeerForSearch(queryEmbed []float64, excludeNodeID []byte) *types.PeerInfo {
	// Hash the embedding to use for routing
	queryHash := k.hashEmbedding(queryEmbed)

	// Use your routing table's FindClosest method to get candidate peers
	closestPeers := k.routingTable.FindClosest(queryHash, k.routingTable.K)

	// Filter out the source node (the one we want to exclude)
	for _, peer := range closestPeers {
		if string(peer.NodeID) != string(excludeNodeID) {
			return &peer
		}
	}

	// No suitable peers found
	return nil
}

// IterativeFindNode performs iterative lookup to find and reach target NodeID
func (k *KademliaNode) IterativeFindNode(targetNodeID []byte) (*types.FindNodeResponse, error) {
	if targetNodeID == nil {
		return nil, fmt.Errorf("target NodeID cannot be nil")
	}

	fmt.Printf("Starting iterative lookup for target: %x\n", targetNodeID[:8])

	// Track nodes we've already queried to avoid loops
	queriedNodes := make(map[string]bool)
	queriedNodes[string(k.NodeID)] = true // Don't query ourselves

	// Get initial closest nodes from our routing table
	closestNodes := k.routingTable.FindClosest(targetNodeID, k.routingTable.K)
	if len(closestNodes) == 0 {
		return nil, fmt.Errorf("no nodes in routing table to start lookup")
	}

	fmt.Printf("Starting with %d initial nodes from routing table\n", len(closestNodes))

	// Track the closest nodes found so far
	allFoundNodes := make(map[string]types.PeerInfo)

	// Add initial nodes
	for _, peer := range closestNodes {
		allFoundNodes[string(peer.NodeID)] = peer
	}

	maxIterations := 10 // Prevent infinite loops
	iteration := 0

	for iteration < maxIterations {
		iteration++
		fmt.Printf("Iteration %d: Querying nodes...\n", iteration)

		// Check if we've reached the target
		if _, exists := allFoundNodes[string(targetNodeID)]; exists {
			fmt.Printf("✓ Target node found in iteration %d!\n", iteration)
			return &types.FindNodeResponse{
				SenderNodeID: targetNodeID,
				SenderPeerID: allFoundNodes[string(targetNodeID)].PeerID,
				ClosestNodes: k.getKClosestNodes(targetNodeID, allFoundNodes),
				Timestamp:    time.Now().UnixNano(),
				Success:      true,
			}, nil
		}

		// Find unqueried nodes to ask
		nodesToQuery := k.selectNodesToQuery(targetNodeID, allFoundNodes, queriedNodes)
		if len(nodesToQuery) == 0 {
			fmt.Printf("No more nodes to query. Lookup complete.\n")
			break
		}

		fmt.Printf("Querying %d nodes in this iteration\n", len(nodesToQuery))

		// Query multiple nodes in parallel (simplified sequential for now)
		newNodesFound := false
		for _, nodeToQuery := range nodesToQuery {
			fmt.Printf("  Querying node: %x\n", nodeToQuery.NodeID[:8])

			response, err := k.FindNode(nodeToQuery.NodeID, targetNodeID)
			queriedNodes[string(nodeToQuery.NodeID)] = true

			if err != nil {
				fmt.Printf("  Failed to query node %x: %v\n", nodeToQuery.NodeID[:8], err)
				continue
			}

			// Add new nodes from response
			for _, newNode := range response.ClosestNodes {
				if _, exists := allFoundNodes[string(newNode.NodeID)]; !exists {
					allFoundNodes[string(newNode.NodeID)] = newNode
					newNodesFound = true
					fmt.Printf("  Found new node: %x\n", newNode.NodeID[:8])
				}
			}
		}

		// If no new nodes found, we're done
		if !newNodesFound {
			fmt.Printf("No new nodes discovered. Lookup complete.\n")
			break
		}
	}

	// Return the closest nodes we found
	closestFound := k.getKClosestNodes(targetNodeID, allFoundNodes)

	return &types.FindNodeResponse{
		SenderNodeID: k.NodeID,
		SenderPeerID: k.PeerID,
		ClosestNodes: closestFound,
		Timestamp:    time.Now().UnixNano(),
		Success:      len(closestFound) > 0,
	}, nil
}

// selectNodesToQuery selects the best nodes to query next in iterative lookup
func (k *KademliaNode) selectNodesToQuery(targetID []byte, allNodes map[string]types.PeerInfo, queriedNodes map[string]bool) []types.PeerInfo {
	var candidates []types.PeerInfo

	// Get unqueried nodes
	for nodeIDStr, peer := range allNodes {
		if !queriedNodes[nodeIDStr] {
			candidates = append(candidates, peer)
		}
	}

	// Sort by distance to target
	sort.Slice(candidates, func(i, j int) bool {
		distI := helpers.XORDistance(targetID, candidates[i].NodeID)
		distJ := helpers.XORDistance(targetID, candidates[j].NodeID)
		return distI.Cmp(distJ) < 0
	})

	// Return closest α nodes (typically 3)
	alpha := 3
	if len(candidates) < alpha {
		return candidates
	}
	return candidates[:alpha]
}

// getKClosestNodes returns K closest nodes from discovered nodes
func (k *KademliaNode) getKClosestNodes(targetID []byte, allNodes map[string]types.PeerInfo) []types.PeerInfo {
	var nodes []types.PeerInfo
	for _, peer := range allNodes {
		nodes = append(nodes, peer)
	}

	// Sort by distance to target
	sort.Slice(nodes, func(i, j int) bool {
		distI := helpers.XORDistance(targetID, nodes[i].NodeID)
		distJ := helpers.XORDistance(targetID, nodes[j].NodeID)
		return distI.Cmp(distJ) < 0
	})

	// Return K closest
	k_value := k.routingTable.K
	if len(nodes) < k_value {
		return nodes
	}
	return nodes[:k_value]
}

// hashEmbedding converts an embedding vector to a hash for routing decisions
func (k *KademliaNode) hashEmbedding(embedding []float64) []byte {
	data := make([]byte, len(embedding)*8)
	for i, val := range embedding {
		bits := math.Float64bits(val)
		binary.LittleEndian.PutUint64(data[i*8:(i+1)*8], bits)
	}

	hasher := sha1.New()
	hasher.Write(data)
	return hasher.Sum(nil)
}

func (k *KademliaNode) CompleteEmbeddingLookup(queryEmbed []float64) (*types.EmbeddingSearchResponse, error) {
	fmt.Printf("=== Starting Complete Embedding Lookup ===\n")

	// STEP 1: Use embedding search to find target NodeID
	fmt.Printf("Step 1: Finding target NodeID using embedding similarity...\n")

	// Use FindSimilar - get the best match from the list
	closestNodes, err := k.FindSimilar(queryEmbed, 0.0, k.routingTable.K)
	if err != nil {
		return nil, fmt.Errorf("failed to find similar embeddings: %w", err)
	}

	if len(closestNodes) == 0 {
		return nil, fmt.Errorf("no similar embeddings found in local storage")
	}

	// Use the best match (first element)
	targetNodeID := closestNodes[0].Key
	fmt.Printf("Step 1 Complete: Target NodeID found: %x (similarity: %.3f)\n",
		targetNodeID[:8], closestNodes[0].Similarity)

	// Rest of the function remains the same...
	// STEP 2: Use iterative FIND_NODE to route to target
	fmt.Printf("Step 2: Routing to target NodeID using Kademlia...\n")

	lookupResponse, err := k.IterativeFindNode(targetNodeID)
	if err != nil {
		return nil, fmt.Errorf("failed to reach target node: %w", err)
	}

	fmt.Printf("Step 2 Complete: Lookup finished\n")

	// STEP 3: Check if we actually reached the target
	var finalResponse *types.EmbeddingSearchResponse

	if string(lookupResponse.SenderNodeID) == string(targetNodeID) {
		// We reached the target! It responds with the embedding
		fmt.Printf("Step 3: Successfully reached target node!\n")

		finalResponse = &types.EmbeddingSearchResponse{
			QueryType:    "complete_lookup",
			QueryEmbed:   queryEmbed,
			Depth:        1,
			SourceNodeID: targetNodeID,
			SourcePeerID: lookupResponse.SenderPeerID,
			NextNodeID:   targetNodeID,
			Found:        true,
			FileEmbed:    closestNodes[0].Embedding,
		}
	} else {
		// We got close but didn't reach the exact target
		fmt.Printf("Step 3: Reached closest available node\n")

		var nextNodeID []byte
		if len(lookupResponse.ClosestNodes) > 0 {
			nextNodeID = lookupResponse.ClosestNodes[0].NodeID
		} else {
			nextNodeID = lookupResponse.SenderNodeID
		}

		finalResponse = &types.EmbeddingSearchResponse{
			QueryType:    "complete_lookup",
			QueryEmbed:   queryEmbed,
			Depth:        1,
			SourceNodeID: lookupResponse.SenderNodeID,
			SourcePeerID: lookupResponse.SenderPeerID,
			NextNodeID:   nextNodeID,
			Found:        false,
			FileEmbed:    nil,
		}
	}

	fmt.Printf("=== Complete Embedding Lookup Finished ===\n")
	return finalResponse, nil
}

// HandleEmbeddingSearch processes incoming embedding search requests
func (k *KademliaNode) HandleEmbeddingSearch(req *types.EmbeddingSearchRequest) (*types.EmbeddingSearchResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("embedding search request cannot be nil")
	}

	// Update routing table with source node
	if len(req.SourceNodeID) > 0 {
		sourcePeer := types.PeerInfo{
			NodeID: req.SourceNodeID,
			PeerID: req.SourcePeerID,
		}
		k.routingTable.Update(sourcePeer)
	}

	// Find closest node embeddings from local database using FindSimilar
	closestNodes, err := k.FindSimilar(req.QueryEmbed, req.Threshold, req.ResultsCount)
	if err != nil {
		return nil, fmt.Errorf("database search failed: %w", err)
	}

	response := &types.EmbeddingSearchResponse{
		QueryType:    req.QueryType,
		QueryEmbed:   req.QueryEmbed,
		Depth:        req.Depth + 1,
		SourceNodeID: k.NodeID,
		SourcePeerID: k.PeerID,
		Found:        len(closestNodes) > 0,
		NextNodeID:   nil,
	}

	// If we found similar embeddings, return the best one
	if len(closestNodes) > 0 {
		response.FileEmbed = closestNodes[0].Embedding
		response.NextNodeID = closestNodes[0].Key
		return response, nil
	}

	// If no nodes in database, try to find next node from routing table
	nextPeer := k.findNextPeerForSearch(req.QueryEmbed, req.SourceNodeID)
	if nextPeer != nil {
		response.NextNodeID = nextPeer.NodeID
	}

	return response, nil
}
