package helpers

import (
	"fmt"
	"log"
	"math"
	"math/big"
	"sync"
)

// EmbeddingProcessor handles cosine similarity calculations for preprocessed embeddings
type EmbeddingProcessor struct {
	// Cache for storing similarity calculations to avoid recomputation
	Cache      map[string]float64
	CacheMutex sync.RWMutex

	// Default threshold for similarity matching
	DefaultThreshold float64
}

// NewEmbeddingProcessor creates a new processor instance
func NewEmbeddingProcessor(defaultThreshold float64) *EmbeddingProcessor {
	return &EmbeddingProcessor{
		Cache:            make(map[string]float64),
		DefaultThreshold: defaultThreshold,
	}
}

// ProcessEmbedding - since you receive preprocessed embeddings, this just validates and returns them
func (ep *EmbeddingProcessor) ProcessEmbedding(input interface{}) ([]float64, error) {
	switch v := input.(type) {
	case []float64:
		// Input is already a preprocessed embedding
		return v, nil
	case []float32:
		// Convert float32 to float64 if needed
		result := make([]float64, len(v))
		for i, val := range v {
			result[i] = float64(val)
		}
		return result, nil
	default:
		return nil, fmt.Errorf("unsupported input type: %T, expected []float64 or []float32", input)
	}
}

// CosineSimilarity calculates the cosine similarity between two embedding vectors
func (ep *EmbeddingProcessor) CosineSimilarity(a, b []float64) (float64, error) {
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

// FindSimilarEmbeddings finds embeddings above a certain similarity threshold
func (ep *EmbeddingProcessor) FindSimilarEmbeddings(queryEmbedding []float64, candidates [][]float64, threshold float64) ([]SimilarityResult, error) {
	var results []SimilarityResult

	for i, candidate := range candidates {
		similarity, err := ep.CosineSimilarity(queryEmbedding, candidate)
		if err != nil {
			continue // Skip invalid embeddings
		}

		if similarity >= threshold {
			results = append(results, SimilarityResult{
				Index:      i,
				Embedding:  candidate,
				Similarity: similarity,
			})
		}
	}

	return results, nil
}

// FindMostSimilar finds the most similar embedding from a set of candidates
func (ep *EmbeddingProcessor) FindMostSimilar(queryEmbedding []float64, candidates [][]float64) (*SimilarityResult, error) {
	if len(candidates) == 0 {
		return nil, fmt.Errorf("no candidate embeddings provided")
	}

	bestResult := &SimilarityResult{
		Index:      -1,
		Similarity: -2.0, // Start with value lower than minimum possible similarity
	}

	for i, candidate := range candidates {
		similarity, err := ep.CosineSimilarity(queryEmbedding, candidate)
		if err != nil {
			continue // Skip invalid embeddings
		}

		if similarity > bestResult.Similarity {
			bestResult.Index = i
			bestResult.Embedding = candidate
			bestResult.Similarity = similarity
		}
	}

	if bestResult.Index == -1 {
		return nil, fmt.Errorf("no valid candidate embeddings found")
	}

	return bestResult, nil
}

// SimilarityResult represents a similarity calculation result
type SimilarityResult struct {
	Index      int       `json:"index"`
	Embedding  []float64 `json:"embedding"`
	Similarity float64   `json:"similarity"`
}

// EmbeddingPair represents a pair of embeddings for batch processing
type EmbeddingPair struct {
	A []float64
	B []float64
}

func XORDistance(a, b []byte) *big.Int {
	var sa, sb string
	sa = string(a)
	sb = string(b)
	if len(a) != len(b) {
		log.Printf("Error. Lengths are %v for %v, %v for %v", len(a),sa, len(b),sb)
		panic("")
	}

	dist := make([]byte, len(a))
	for i := range a {
		dist[i] = a[i] ^ b[i]
	}

	return new(big.Int).SetBytes(dist)
}

func BucketIndex(selfID, otherID []byte) int {
	// if len(selfID) != len(otherID) {
	// 	panic("IDs must be the same length 2")
	// }

	for byteIndex := range selfID {
		xorByte := selfID[byteIndex] ^ otherID[byteIndex]

		if xorByte != 0 {
			for bitPos := range 8 {
				if (xorByte & (0x80 >> bitPos)) != 0 {
					return (len(selfID)-byteIndex-1)*8 + (7 - bitPos)
				}
			}
		}
	}
	return -1
}

// func RandomNodeID() []byte {
// 	id := make([]byte, identity.NodeIDBytes)
// 	if _, err := rand.Read(id); err != nil {
// 		log.Fatalf("failed to generate random NodeID: %v", err)
// 	}
// 	return id
// }
