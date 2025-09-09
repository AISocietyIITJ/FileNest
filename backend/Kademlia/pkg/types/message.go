package types

// contains all the structs for the messages used in the Kademlia protocol

type PeerInfo struct {
	NodeID []byte `json:"node_id"`
	PeerID string `json:"peer_id"`
}

type EmbeddingSearchRequest struct {
	SourceNodeID []byte    `json:"source_id"`
	SourcePeerID string    `json:"source_peer_id"`
	QueryEmbed   []float64 `json:"embed"`
	Depth        int       `json:"prev_depth"`
	QueryType    string    `json:"query_type"`
	Threshold    float64   `json:"threshold"`
	ResultsCount int       `json:"results_count"`
	TargetNodeID []byte    `json:"target_node_id"`
}

type EmbeddingSearchResponse struct {
	SourceNodeID []byte    `json:"source_node_id"`
	SourcePeerID string    `json:"source_peer_id"`
	QueryEmbed   []float64 `json:"query_embed"`
	Depth        int       `json:"depth"`
	QueryType    string    `json:"type"`
	NextNodeID   []byte    `json:"next_node_id"`
	Found        bool      `json:"found"`
	FileEmbed    []float64 `json:"file_embed"`
}

//to be modified with info about receiver
type PingRequest struct {
	SenderNodeID []byte `json:"sender_id"`
	SenderPeerID string `json:"sender_addr"`
	Timestamp    int64  `json:"timestamp"`
}

type PingResponse struct {
	SenderNodeID []byte `json:"sender_id"`
	SenderPeerID string `json:"sender_addr"`
	Timestamp    int64  `json:"timestamp"`
	Success      bool   `json:"success"`
}


//sent to kademlia node
type FindNodeRequest struct {
    SenderNodeID []byte `json:"sender_node_id"`
    SenderPeerID string `json:"sender_peer_id"`
    TargetID     []byte `json:"target_id"`    // The NodeID we want to reach
    Timestamp    int64  `json:"timestamp"`
}

//sent back to user for relaying
type FindNodeResponse struct {
    SenderNodeID []byte     `json:"sender_node_id"`
    SenderPeerID string     `json:"sender_peer_id"`
    ClosestNodes []PeerInfo `json:"closest_nodes"`  // K closest nodes to TargetID
    Timestamp    int64      `json:"timestamp"`
    Success      bool       `json:"success"`
}