package integration

import (
	"bytes"
	"database/sql"
	"encoding/hex"
	"final/backend/pkg/helpers"
	"final/backend/pkg/identity"
	"final/backend/pkg/kademlia"
	"final/backend/pkg/types"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	_ "github.com/mattn/go-sqlite3"
)

// Local wrapper type to extend helpers.EmbeddingProcessor
type LocalEmbeddingProcessor struct {
	*helpers.EmbeddingProcessor
}

// NewLocalEmbeddingProcessor creates a wrapper around helpers.EmbeddingProcessor
func NewLocalEmbeddingProcessor() *LocalEmbeddingProcessor {
	return &LocalEmbeddingProcessor{
		EmbeddingProcessor: &helpers.EmbeddingProcessor{},
	}
}

// NetworkIntegrationService handles network layer integration with Kademlia DHT
type NetworkIntegrationService struct {
	kademliaNode       *kademlia.KademliaNode
	embeddingProcessor *LocalEmbeddingProcessor
	hostPeerID         peer.ID
	currentDepth       int
}

// NewNetworkIntegrationService creates a new network integration service
func NewNetworkIntegrationService(node *kademlia.KademliaNode, processor *LocalEmbeddingProcessor, depth int) *NetworkIntegrationService {
	return &NetworkIntegrationService{
		kademliaNode:       node,
		embeddingProcessor: processor,
		hostPeerID:         peer.ID(node.GetAddress()),
		currentDepth:       depth,
	}
}

// ProcessEmbeddingRequest - Main function implementing the requested logic
func (nis *NetworkIntegrationService) ProcessEmbeddingRequest(request *types.EmbeddingSearchRequest) (*types.EmbeddingSearchResponse, error) {
	log.Printf("Processing embedding request: type=%s, target=%x, source=%s, depth=%d",
		request.QueryType, request.TargetNodeID[:8], request.SourcePeerID, request.Depth)

	// Get current node's ID
	myNodeID := nis.kademliaNode.GetID()

	// Check if target node ID is the same as peer's node ID using bytes.Equal
	if bytes.Equal(request.TargetNodeID, myNodeID) {
		log.Printf("Target node matches current peer - finding next node by cosine similarity")
		return nis.findNextNodeBySimilarity(request)
	} else {
		log.Printf("Target node doesn't match - routing via Kademlia to target %x", request.TargetNodeID[:8])
		return nis.routeViaKademlia(request)
	}
}

// findNextNodeBySimilarity - When target matches current node, find next node by similarity
func (nis *NetworkIntegrationService) findNextNodeBySimilarity(request *types.EmbeddingSearchRequest) (*types.EmbeddingSearchResponse, error) {
	log.Printf("Finding next node by cosine similarity for embedding")

	storedEmbeddings, err := nis.kademliaNode.FindSimilar(request.QueryEmbed, 0.0, 100)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve stored embeddings: %w", err)
	}

	if len(storedEmbeddings) == 0 {
		return &types.EmbeddingSearchResponse{
			QueryType:    "no_embeddings",
			QueryEmbed:   request.QueryEmbed,
			Depth:        request.Depth,
			SourceNodeID: nis.kademliaNode.GetID(),
			SourcePeerID: nis.kademliaNode.GetAddress(),
			Found:        false,
			NextNodeID:   nil,
			FileEmbed:    nil,
		}, fmt.Errorf("no embeddings found for similarity comparison")
	}

	var bestMatch *EmbeddingResult
	maxSimilarity := -2.0

	for _, stored := range storedEmbeddings {
		similarity, err := nis.embeddingProcessor.CosineSimilarity(request.QueryEmbed, stored.Embedding)
		if err != nil {
			continue
		}

		if similarity > maxSimilarity {
			maxSimilarity = similarity
			bestMatch = &EmbeddingResult{
				NodeID:     stored.NodeID,
				Embedding:  stored.Embedding,
				Similarity: similarity,
			}
		}
	}

	if bestMatch == nil {
		return &types.EmbeddingSearchResponse{
			QueryType:    "similarity_error",
			QueryEmbed:   request.QueryEmbed,
			Depth:        request.Depth,
			SourceNodeID: nis.kademliaNode.GetID(),
			SourcePeerID: nis.kademliaNode.GetAddress(),
			Found:        false,
			NextNodeID:   nil,
			FileEmbed:    nil,
		}, fmt.Errorf("no valid similarity matches found")
	}

	log.Printf("Found next node by similarity: %x (similarity: %.4f)", bestMatch.NodeID[:8], bestMatch.Similarity)

	return &types.EmbeddingSearchResponse{
		QueryType:    "similarity_match",
		QueryEmbed:   request.QueryEmbed,
		Depth:        request.Depth + 1,
		SourceNodeID: nis.kademliaNode.GetID(),
		SourcePeerID: nis.kademliaNode.GetAddress(),
		Found:        true,
		NextNodeID:   bestMatch.NodeID,
		FileEmbed:    bestMatch.Embedding,
	}, nil
}

// routeViaKademlia - When target doesn't match, route via Kademlia DHT
func (nis *NetworkIntegrationService) routeViaKademlia(request *types.EmbeddingSearchRequest) (*types.EmbeddingSearchResponse, error) {
	log.Printf("Routing to target node %x via Kademlia", request.TargetNodeID[:8])

	rt := nis.kademliaNode.RoutingTable()
	closestPeers := rt.FindClosest(request.TargetNodeID, rt.K)

	if len(closestPeers) == 0 {
		return &types.EmbeddingSearchResponse{
			QueryType:    "routing_error",
			QueryEmbed:   request.QueryEmbed,
			Depth:        request.Depth,
			SourceNodeID: nis.kademliaNode.GetID(),
			SourcePeerID: nis.kademliaNode.GetAddress(),
			Found:        false,
			NextNodeID:   nil,
			FileEmbed:    nil,
		}, fmt.Errorf("no peers available for routing to target %x", request.TargetNodeID[:8])
	}

	nextHop := closestPeers[0]
	log.Printf("Routing to next hop: %s (node ID: %x)", nextHop.PeerID, nextHop.NodeID[:8])

	return &types.EmbeddingSearchResponse{
		QueryType:    "routed",
		QueryEmbed:   request.QueryEmbed,
		Depth:        request.Depth,
		SourceNodeID: nis.kademliaNode.GetID(),
		SourcePeerID: nis.kademliaNode.GetAddress(),
		Found:        false,
		NextNodeID:   nextHop.NodeID,
		FileEmbed:    nil,
	}, nil
}

// Supporting type for embedding results
type EmbeddingResult struct {
	NodeID     []byte    `json:"node_id"`
	Embedding  []float64 `json:"embedding"`
	Similarity float64   `json:"similarity"`
}

// ========== COMPREHENSIVE WRAPPER FUNCTIONS ==========

// ComprehensiveKademliaHandler - Single wrapper that handles all Kademlia operations
type ComprehensiveKademliaHandler struct {
	node           *kademlia.KademliaNode
	networkService *NetworkIntegrationService
	isInitialized  bool
}

// NewComprehensiveKademliaHandler creates the handler
func NewComprehensiveKademliaHandler() *ComprehensiveKademliaHandler {
	return &ComprehensiveKademliaHandler{
		isInitialized: false,
	}
}

func (ckh *ComprehensiveKademliaHandler) Node() *kademlia.KademliaNode{
	return ckh.node;
} 

// InitializeNode - Initialize Kademlia node
func (ckh *ComprehensiveKademliaHandler) InitializeNode(peerID, dbPath string) ([]byte,error) {
	if ckh.isInitialized {
		return nil, nil
	}

	// ✅ Convert ONCE at initialization
	nodeID, err := identity.LoadOrCreateNodeID("")
	if err != nil {
		return nil, fmt.Errorf("failed to load or create node ID: %w", err)
	}

	var network kademlia.NetworkInterface
	log.Printf("network integration, nodeLen %v", len(nodeID))
	node, err := kademlia.NewKademliaNode(nodeID, peerID, network, dbPath)
	if err != nil {
		return nodeID, fmt.Errorf("failed to create Kademlia node: %w", err)
	}

	processor := NewLocalEmbeddingProcessor()
	networkService := NewNetworkIntegrationService(node, processor, 0)

	// add code for loading routing table from the database.
	// if the database doesn't exist, it should be created and the routing table should be

	ckh.node = node
	ckh.networkService = networkService
	ckh.isInitialized = true

	log.Printf("Kademlia node initialized: %x", nodeID[:8])

	// --- Routing Table DB Logic ---
	routingDBPath := "routing_table.db"
	var db *sql.DB
	if _, err := os.Stat(routingDBPath); os.IsNotExist(err) {
		db, err = sql.Open("sqlite3", routingDBPath)
		if err != nil {
			return nodeID, fmt.Errorf("failed to open routing table db: %w", err)
		}
		defer db.Close()

		// Create table if not exists
		_, err = db.Exec(`CREATE TABLE IF NOT EXISTS routing_table (
			node_id TEXT PRIMARY KEY,
			peer_id TEXT
		)`)
		if err != nil {
			return nodeID, fmt.Errorf("failed to create routing table: %w", err)
		}
		log.Printf("Created routing_table.db and routing_table table.")
	} else {
		db, err = sql.Open("sqlite3", routingDBPath)
		if err != nil {
			return nodeID, fmt.Errorf("failed to open routing table db: %w", err)
		}
		defer db.Close()
	}

	// Load existing entries into routing table
	rows, err := db.Query("SELECT node_id, peer_id FROM routing_table")
	if err != nil {
		return nodeID, fmt.Errorf("failed to query routing table: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var nodeIDHex, peerID string
		if err := rows.Scan(&nodeIDHex, &peerID); err != nil {
			return nodeID,fmt.Errorf("failed to scan routing table row: %w", err)
		}
		nodeIDBytes, err := hex.DecodeString(nodeIDHex)
		if err != nil {
			log.Printf("Invalid node_id in routing table: %s", nodeIDHex)
			continue
		}
		peerInfo := types.PeerInfo{NodeID: nodeIDBytes, PeerID: peerID}
		node.RoutingTable().Update(peerInfo)
	}

	return nodeID, nil
}

// ProcessEmbeddingRequestWrapper - Main function using []byte throughout
func (ckh *ComprehensiveKademliaHandler) ProcessEmbeddingRequestWrapper(
	queryEmbed []float64,
	targetNodeID []byte, // ✅ []byte parameter
	queryType string,
	threshold float64,
	resultsCount int,
) (*types.EmbeddingSearchResponse, error) {

	if !ckh.isInitialized {
		return nil, fmt.Errorf("node not initialized - call InitializeNode first")
	}

	request := &types.EmbeddingSearchRequest{
		SourceNodeID: ckh.node.GetID(),
		SourcePeerID: ckh.node.GetAddress(),
		QueryEmbed:   queryEmbed,
		Depth:        0,
		QueryType:    queryType,
		Threshold:    threshold,
		ResultsCount: resultsCount,
		TargetNodeID: targetNodeID, // ✅ Direct use - no conversion
	}

	return ckh.networkService.ProcessEmbeddingRequest(request)
}


// StoreEmbedding - Store an embedding using []byte node ID
func (ckh *ComprehensiveKademliaHandler) StoreEmbedding(targetNodeID []byte, peerID string,embedding []float64) error {
	if !ckh.isInitialized {
		return fmt.Errorf("node not initialized")
	}

	log.Printf("[StoreEmbedding] NodeID length: %d", len(targetNodeID))

	// ✅ Direct use - no conversion
	return ckh.node.StoreNodeEmbedding(targetNodeID, peerID, embedding)
}


// GetNodeStatistics - Get node statistics
func (ckh *ComprehensiveKademliaHandler) GetNodeStatistics() map[string]interface{} {
	if !ckh.isInitialized {
		return map[string]interface{}{
			"initialized": false,
			"error":       "node not initialized",
		}
	}
	log.Printf("[GetNodeStatistics] NodeID: %v", string(ckh.node.GetID()))
	log.Printf("[GetNodeStatistics] NodeID length: %d", len(ckh.node.GetID()))

	rt := ckh.node.RoutingTable()
	stats := map[string]interface{}{
		"node_id":       fmt.Sprintf("%x", ckh.node.GetID()), // !!! was an [:8] here.
		"peer_id":       ckh.node.GetAddress(),
		"routing_peers": len(rt.FindClosest(ckh.node.GetID(), rt.K)),
		"initialized":   ckh.isInitialized,
		"timestamp":     time.Now().Unix(),
	}
	log.Printf("Node Stats: %+v", stats)
	return stats
}

// AddPeerToRoutingTable adds a peer directly to the Kademlia routing table
func (ckh *ComprehensiveKademliaHandler) AddPeerToRoutingTable(peer types.PeerInfo) error {
	if !ckh.isInitialized {
		return fmt.Errorf("kademlia handler not initialized")
	}

	if ckh.node == nil {
		return fmt.Errorf("kademlia node is nil")
	}

	log.Printf("[AddPeerToRoutingTable] NodeID length: %d", len(ckh.node.GetID()))

	routingTable := ckh.node.RoutingTable()
	if routingTable == nil {
		return fmt.Errorf("routing table is nil")
	}

	// Call Update method
	routingTable.Update(peer)

	log.Printf("Added peer to routing table: NodeID=%x, PeerID=%s",
		peer.NodeID, peer.PeerID)

	return nil
}

// GetRoutingInfo - Get routing table information
func (ckh *ComprehensiveKademliaHandler) GetRoutingInfo() []types.PeerInfo {
	if !ckh.isInitialized || ckh.node == nil {
		return nil
	}

	routingTable := ckh.node.RoutingTable()
	if routingTable == nil {
		return nil
	}

	return routingTable.GetNodes()
}