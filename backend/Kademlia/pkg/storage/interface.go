package storage

type EmbeddingResult struct {
	NodeID     []byte    `json:"node_id"`
	PeerID     string    `json:"peer_id"`
	Embedding  []float64 `json:"embedding"`
	Similarity float64   `json:"similarity"`
}

type Interface interface {
	StoreNodeEmbedding(nodeID []byte, peerID string, embeddingVec []float64) error
	FindSimilar(queryEmbed []float64, threshold float64, limit int) ([]EmbeddingResult, error)
}
