package models

import (
	"encoding/json"
	"final/backend/pkg/types"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

type UserPeer struct {
    Host      host.Host
    RelayAddr multiaddr.Multiaddr
    RelayID   peer.ID
    Peers     map[peer.ID]string // peer ID to nickname mapping
}

type ReqFormat struct {
    Type      string          `json:"type,omitempty"`
    PeerID    string          `json:"peer_id,omitempty"`
    ReqParams json.RawMessage `json:"req_params,omitempty"`
    Body      json.RawMessage `json:"body,omitempty"`
}

type EmbeddingStoreRequest struct {
    Route          string    `json:"route,omitempty"`
    Type           string    `json:"type,omitempty"`
    SourceNodeID   string    `json:"source_node_id,omitempty"`
    SourcePeerID   string    `json:"source_peer_id,omitempty"`
    ReceiverPeerID string    `json:"receiver_peer_id,omitempty"`
    FileEmbed      []float64 `json:"file_embed,omitempty"`
    FilePath       string    `json:"file_path,omitempty"`
    Depth          int       `json:"depth,omitempty"`
    Threshold      float64   `json:"threshold,omitempty"`
    ResultsCount   int       `json:"results_count,omitempty"`
    TargetNodeID   string    `json:"target_node_id,omitempty"`
    Found          bool      `json:"found,omitempty"`
}

type EmbeddingStoreResponse struct {
    Message      string    `json:"message,omitempty"`
    FileEmbed    []float64 `json:"file_embed,omitempty"`
    Depth        int       `json:"depth,omitempty"`
    SourceNodeID string    `json:"source_node_id,omitempty"`
    SourcePeerID string    `json:"source_peer_id,omitempty"`
    NextNodeID   string    `json:"next_node_id,omitempty"`
    Found        bool      `json:"found,omitempty"`
    Pruned       bool      `json:"pruned,omitempty"`
}

type EmbeddingSearchRequest struct {
    Route          string    `json:"route,omitempty"`
    Type           string    `json:"type,omitempty"`
    SourceNodeID   string    `json:"source_node_id,omitempty"`
    SourcePeerID   string    `json:"source_peer_id,omitempty"`
    ReceiverPeerID string    `json:"receiver_peer_id,omitempty"`
    QueryEmbed     []float64 `json:"query_embed,omitempty"`
    Depth          int       `json:"depth,omitempty"`
    Threshold      float64   `json:"threshold,omitempty"`
    ResultsCount   int       `json:"results_count,omitempty"`
    TargetNodeID   string    `json:"target_node_id,omitempty"`
    Found          bool      `json:"found,omitempty"`
}

type EmbeddingSearchResponse struct {
    Message      string      `json:"message,omitempty"`
    Depth        int         `json:"depth,omitempty"`
    SourceNodeID string      `json:"source_node_id,omitempty"`
    SourcePeerID string      `json:"source_peer_id,omitempty"`
    NextNodeID   string      `json:"next_node_id,omitempty"`
    Found        bool        `json:"found,omitempty"`
    Pruned       bool        `json:"pruned,omitempty"`
    FileEmbeds   [][]float64 `json:"file_embed,omitempty"`
}

type PingRequest struct {
    Type           string `json:"type,omitempty"`
    Route          string `json:"route,omitempty"`
    ReceiverPeerID string `json:"receiver_peer_id,omitempty"`
    SenderNodeID   []byte `json:"sender_node_id,omitempty"`
    SenderPeerID   string `json:"sender_peer_id,omitempty"`
    ReceiverNodeID []byte `json:"receiver_node_id,omitempty"`
    Timestamp      int64  `json:"timestamp,omitempty"`
}

type PingResponse struct {
    SenderNodeID []byte `json:"sender_node_id,omitempty"`
    SenderPeerID string `json:"sender_peer_id,omitempty"`
    Timestamp    int64  `json:"timestamp,omitempty"`
    Success      bool   `json:"success,omitempty"`
}

// sent to kademlia node
type FindNodeRequest struct {
    Type           string `json:"type,omitempty"`
    Route          string `json:"route,omitempty"`
    SenderNodeID   string `json:"sender_node_id,omitempty"`
    SenderPeerID   string `json:"sender_peer_id,omitempty"`
    ReceiverNodeID string `json:"receiver_node_id,omitempty"`
    TargetNodeID   string `json:"target_node_id,omitempty"` // The NodeID we want to reach
    Timestamp      int64  `json:"timestamp,omitempty"`
}

// sent back to user for relaying
type FindNodeResponse struct {
    SenderNodeID string         `json:"sender_node_id,omitempty"`
    SenderPeerID string         `json:"sender_peer_id,omitempty"`
    ClosestNodes []types.PeerInfo `json:"closest_nodes,omitempty"` // K closest nodes to TargetID
    Timestamp    int64          `json:"timestamp,omitempty"`
    Found        bool           `json:"found,omitempty"`
}