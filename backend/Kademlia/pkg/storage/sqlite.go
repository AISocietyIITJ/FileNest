package storage

import (
	// "final/backend/pkg/embedding"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"sort"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type ConfigData struct {
	Embedding []float64 `json:"embedding"`
}

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
		if similarity >= threshold {
			results = append(results, EmbeddingResult{
				NodeID:     ne.NodeID,
				// PeerID:     ne.PeerID,
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
		PeerID:    peerID,
		Embedding: EmbeddingVector(embeddingVec),
	}

	// Create a new record for each embedding. This allows multiple embeddings per NodeID.
	result := s.db.Create(&nodeEmbedding)
	return result.Error
}

func (s *SQLiteStorage) UpdateTV(depth int) ([]float64, error) {
	// Load all embeddings from DB
	var nodeEmbeddings []D1TVMap
	if err := s.db.Find(&nodeEmbeddings).Error; err != nil {
		log.Printf("Error loading embeddings from DB: %v", err)
	}

	if len(nodeEmbeddings) == 0 {
		// nothing to update
		log.Print("UpdatedTV: no embeddings found in DB")
	}

	// determine embedding length from first record
	embLen := len(nodeEmbeddings[0].Embedding)
	// accumulator
	mean := make([]float64, embLen)
	count := 0

	for _, ne := range nodeEmbeddings {
		// skip embeddings with mismatched length
		if len(ne.Embedding) != embLen {
			log.Printf("skipping embedding with mismatched length: got %d expected %d", len(ne.Embedding), embLen)
			continue
		}
		for i, v := range ne.Embedding {
			mean[i] += v
		}
		count++
	}

	if count == 0 {
		log.Printf("UpdatedTV: no embeddings with consistent length found")
	}

	// divide by count to get mean
	for i := range mean {
		mean[i] = mean[i] / float64(count)
	}
	return mean, nil
}

func (s *SQLiteStorage) UpdateConfig(depth int, filepath string) error {
	// Compute the new mean embedding
	newembed, err := s.UpdateTV(depth)
	if err != nil {
		return fmt.Errorf("UpdatedTV failed: %w", err)
	}

	cfg := ConfigData{Embedding: newembed}

	// Check file existence
	if _, err := os.Stat(filepath); os.IsNotExist(err) {
		// File does not exist, create it with initial data
		fmt.Printf("File '%s' does not exist. Creating it.\n", filepath)
		jsonData, err := json.MarshalIndent(cfg, "", "  ")
		if err != nil {
			return fmt.Errorf("marshal initial config: %w", err)
		}
		if err := os.WriteFile(filepath, jsonData, 0644); err != nil {
			return fmt.Errorf("write initial config: %w", err)
		}
		fmt.Printf("File '%s' created successfully with initial data.\n", filepath)
		return nil
	} else if err != nil {
		// Some other stat error
		return fmt.Errorf("stat config file: %w", err)
	}

	// File exists -> read and update
	data, err := os.ReadFile(filepath)
	if err != nil {
		return fmt.Errorf("read existing config: %w", err)
	}

	var existing ConfigData
	if len(data) > 0 {
		if err := json.Unmarshal(data, &existing); err != nil {
			log.Printf("Warning: could not unmarshal existing config, will overwrite: %v", err)
			existing = ConfigData{}
		}
	}

	existing.Embedding = newembed
	jsonData, err := json.MarshalIndent(existing, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal updated config: %w", err)
	}
	if err := os.WriteFile(filepath, jsonData, 0644); err != nil {
		return fmt.Errorf("write updated config: %w", err)
	}
	fmt.Printf("File '%s' updated successfully with new embedding.\n", filepath)
	return nil
}

// upsert into D1/2/3/4TVMap based on depth. also create a new db if not exists. also create a config file corresponding to the embedding of the depth

// func (s *SQLiteStorage) UpsertNodeEmbedding(nodeID []byte, peerID string, embeddingVec []float64, depth int) error {
// 	if len(nodeID) != 20 {
// 		return fmt.Errorf("nodeID must be 20 bytes (160 bits), got %d", len(nodeID))
// 	}
// 	nodeEmbedding := D1TVMap{
//         NodeID:    nodeID,
// 		PeerID: peerID,
//         Embedding: EmbeddingVector(embeddingVec),
//     }

//     // Create a new record for each embedding. This allows multiple embeddings per NodeID.
//     result := s.db.Create(&nodeEmbedding)
//     return result.Error

// }
// retrieve records based on depth

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
