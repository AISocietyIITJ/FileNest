package kademlia

import "final/backend/pkg/types"

// Main Kademlia interface that network layer will use
type Kademlia interface {
    // Handle incoming requests
    // HandlePing(req *types.PingRequest) (*types.PingResponse, error)
    // HandleEmbeddingSearch(req *types.EmbeddingSearchRequest) (*types.EmbeddingSearchResponse, error)
    
    // Initiate outbound requests
    Ping(targetNodeID []byte) (*types.PingResponse, error)
    // EmbeddingSearch(req *types.EmbeddingSearchRequest) (*types.EmbeddingSearchResponse, error)
}

// Network callback interface - how Kademlia talks back to network layer
type NetworkInterface interface {
    SendPing(targetNodeID []byte, req *types.PingRequest) (*types.PingResponse, error)
    // SendEmbeddingSearch(targetNodeID []byte, req *types.EmbeddingSearchRequest) (*types.EmbeddingSearchResponse, error)
    SendFindNode(targetNodeID []byte, req *types.FindNodeRequest) (*types.FindNodeResponse, error)
}