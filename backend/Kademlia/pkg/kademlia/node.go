package kademlia

import (

	// "final/backend/pkg/helpers"
	"final/backend/pkg/storage"
	"fmt"
	"math"
	// "sort"
)

type KademliaNode struct {
	// RoutingTable returns the node's routing table (exported getter)
	NodeID       []byte            // Persistent NodeID
	PeerID       string            // Ephemeral libp2p PeerID
	routingTable *RoutingTable     // stores nodeIDs which have contacted the Node before
	storage      storage.Interface // stores nodeIDs which match the TV of this node
	D2DB		 storage.Interface
	D3DB		 storage.Interface
	D4DB		 storage.Interface
	IndexedFiles   storage.Interface
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
	D2DB, err := storage.NewSQLiteStorage("./d2tv.db")
	if err != nil {
		return nil, err
	}
	D3DB, err := storage.NewSQLiteStorage("./d3tv.db")
	if err != nil {
		return nil, err
	}
	D4DB, err := storage.NewSQLiteStorage("./d4tv.db")
	if err != nil {
		return nil, err
	}
	IndexedFiles, err := storage.NewSQLiteStorage("./IndexedFiles.db")
	if err != nil {
		return nil, err
	}

	return &KademliaNode{
		NodeID:       nodeID,
		PeerID:       peerID,
		routingTable: NewRoutingTable(nodeID, peerID, 20), // K=20
		storage:      sqliteStorage,                       // Initialize D1 Storage
		D2DB: D2DB,
		D3DB: D3DB,
		D4DB: D4DB,
		IndexedFiles: IndexedFiles,
		network:      network,
	}, nil
}

func (k *KademliaNode) RoutingTable() *RoutingTable {
	return k.routingTable
}

// Storage wrapper functions - Add these to your node.go
func (k *KademliaNode) StoreNodeEmbedding(nodeID []byte, peerID string, embedding []float64) error {
	return k.storage.StoreNodeEmbedding(nodeID, peerID, embedding)
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

// // findNextPeerForSearch finds the best next peer for embedding search routing
// func (k *KademliaNode) findNextPeerForSearch(queryEmbed []float64, excludeNodeID []byte) *types.PeerInfo {
// 	// Hash the embedding to use for routing
// 	queryHash := k.hashEmbedding(queryEmbed)

// 	// Use your routing table's FindClosest method to get candidate peers
// 	closestPeers := k.routingTable.FindClosest(queryHash, k.routingTable.K)

// 	// Filter out the source node (the one we want to exclude)
// 	for _, peer := range closestPeers {
// 		if string(peer.NodeID) != string(excludeNodeID) {
// 			return &peer
// 		}
// 	}

// 	// No suitable peers found
// 	return nil
// }


// // hashEmbedding converts an embedding vector to a hash for routing decisions
// func (k *KademliaNode) hashEmbedding(embedding []float64) []byte {
// 	data := make([]byte, len(embedding)*8)
// 	for i, val := range embedding {
// 		bits := math.Float64bits(val)
// 		binary.LittleEndian.PutUint64(data[i*8:(i+1)*8], bits)
// 	}

// 	hasher := sha1.New()
// 	hasher.Write(data)
// 	return hasher.Sum(nil)
// }

// // HandleEmbeddingSearch processes incoming embedding search requests
// func (k *KademliaNode) HandleEmbeddingSearch(req *types.EmbeddingSearchRequest) (*types.EmbeddingSearchResponse, error) {
// 	if req == nil {
// 		return nil, fmt.Errorf("embedding search request cannot be nil")
// 	}

// 	// Update routing table with source node
// 	if len(req.SourceNodeID) > 0 {
// 		sourcePeer := types.PeerInfo{
// 			NodeID: req.SourceNodeID,
// 			PeerID: req.SourcePeerID,
// 		}
// 		k.routingTable.Update(sourcePeer)
// 	}

// 	// Find closest node embeddings from local database using FindSimilar
// 	closestNodes, err := k.FindSimilar(req.QueryEmbed, req.Threshold, req.ResultsCount)
// 	if err != nil {
// 		return nil, fmt.Errorf("database search failed: %w", err)
// 	}

// 	response := &types.EmbeddingSearchResponse{
// 		QueryType:    req.QueryType,
// 		QueryEmbed:   req.QueryEmbed,
// 		Depth:        req.Depth + 1,
// 		SourceNodeID: k.NodeID,
// 		SourcePeerID: k.PeerID,
// 		Found:        len(closestNodes) > 0,
// 		NextNodeID:   nil,
// 	}

// 	// If we found similar embeddings, return the best one
// 	if len(closestNodes) > 0 {
// 		response.FileEmbed = closestNodes[0].Embedding
// 		response.NextNodeID = closestNodes[0].NodeID
// 		return response, nil
// 	}

// 	// If no nodes in database, try to find next node from routing table
// 	nextPeer := k.findNextPeerForSearch(req.QueryEmbed, req.SourceNodeID)
// 	if nextPeer != nil {
// 		response.NextNodeID = nextPeer.NodeID
// 	}

// 	return response, nil
// }
