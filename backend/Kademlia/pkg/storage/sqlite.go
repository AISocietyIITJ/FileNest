package storage

import (
	// "final/backend/pkg/embedding"
	"fmt"
	"log"
	"math"
	"sort"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type SQLiteStorage struct {
	db *gorm.DB
}

func NewSQLiteStorage(dbPath string) (*SQLiteStorage, error) {
	db, err := gorm.Open(sqlite.Open(dbPath), &gorm.Config{})
	if err != nil {
		return nil, err
	}

	// Auto migrate the schema - now using local D1TVMap struct
	err = db.AutoMigrate(&D1TVMap{})
	if err != nil {
		return nil, err
	}

	return &SQLiteStorage{
		db: db,
	}, nil
}

// Find embed map nodes similar to query embed
func (s *SQLiteStorage) FindSimilar(queryEmbed []float64, threshold float64, limit int) ([]EmbeddingResult, error) {
	var nodeEmbeddings []D1TVMap

	// Get all embeddings from database
	result := s.db.Find(&nodeEmbeddings)
	if result.Error != nil {
		return nil, result.Error
	}

	var results []EmbeddingResult

	// Calculate similarity for each stored embedding
	for _, ne := range nodeEmbeddings {
		similarity, _ := cosineSimilarity(queryEmbed, []float64(ne.Embedding))
		log.Printf("Found NodeEmbed in rt: %+v\n", ne)
		// Only include embeddings that meet the threshold
		if similarity >= threshold{
			results = append(results, EmbeddingResult{
				NodeID:        ne.NodeID,
				PeerID: ne.PeerID,
				Embedding:  []float64(ne.Embedding),
				Similarity: similarity,
			})
		}
	}

	// Sort by similarity (highest first)
	sort.Slice(results, func(i, j int) bool {
		return results[i].Similarity > results[j].Similarity
	})

	// Limit results
	if len(results) > limit {
		results = results[:limit]
	}
	log.Printf("results: %v", results)
	return results, nil
}

func (s *SQLiteStorage) Close() error {
	sqlDB, err := s.db.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}

func (s *SQLiteStorage) StoreNodeEmbedding(nodeID []byte, peerID string, embeddingVec []float64) error {
    if len(nodeID) != 20 {
        return fmt.Errorf("nodeID must be 20 bytes (160 bits), got %d", len(nodeID))
    }

    nodeEmbedding := D1TVMap{
        NodeID:    nodeID,
		PeerID: peerID,
        Embedding: EmbeddingVector(embeddingVec),
    }

    // Create a new record for each embedding. This allows multiple embeddings per NodeID.
    result := s.db.Create(&nodeEmbedding)
    return result.Error
}


// CosineSimilarity calculates the cosine similarity between two embedding vectors
func cosineSimilarity(a, b []float64) (float64, error) {
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