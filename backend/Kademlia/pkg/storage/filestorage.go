package storage

import (
	// "final/backend/pkg/embedding"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func NewSQLiteFileStorage(dbPath string) (*SQLiteStorage, error) {
	db, err := gorm.Open(sqlite.Open(dbPath), &gorm.Config{})
	if err != nil {
		return nil, err
	}

	// Auto migrate the schema - now using local D1TVMap struct
	err = db.AutoMigrate(&FileRecord{})
	if err != nil {
		return nil, err
	}

	return &SQLiteStorage{
		db: db,
	}, nil
}

func (s *SQLiteStorage) StoreFileEmbedding(nodeID []byte, peerID string, embeddingVec []float64, filepath string) error {
	if len(nodeID) != 20 {
		return fmt.Errorf("nodeID must be 20 bytes (160 bits), got %d", len(nodeID))
	}

	nodeEmbedding := FileRecord{
		NodeID:    nodeID,
		PeerID:    peerID,
		Embedding: EmbeddingVector(embeddingVec),
		FilePath:  filepath,
	}

	// Create a new record for each embedding. This allows multiple embeddings per NodeID.
	result := s.db.Create(&nodeEmbedding)
	return result.Error
}

func (s *SQLiteStorage) UpdateD4TV(depth int) ([]float64, error) {
	// Load all embeddings from DB
	var nodeEmbeddings []FileRecord
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

func (s *SQLiteStorage) UpdateD4Config(depth int, dbfilepath string) error {
	// Compute the new mean embedding
	newembed, err := s.UpdateD4TV(depth)
	if err != nil {
		return fmt.Errorf("UpdatedTV failed: %w", err)
	}

	cfg := ConfigData{Embedding: newembed}

	// Check file existence
	if _, err := os.Stat(dbfilepath); os.IsNotExist(err) {
		// File does not exist, create it with initial data
		fmt.Printf("File '%s' does not exist. Creating it.\n", dbfilepath)
		jsonData, err := json.MarshalIndent(cfg, "", "  ")
		if err != nil {
			return fmt.Errorf("marshal initial config: %w", err)
		}
		if err := os.WriteFile(dbfilepath, jsonData, 0644); err != nil {
			return fmt.Errorf("write initial config: %w", err)
		}
		fmt.Printf("File '%s' created successfully with initial data.\n", dbfilepath)
		return nil
	} else if err != nil {
		// Some other stat error
		return fmt.Errorf("stat config file: %w", err)
	}

	// File exists -> read and update
	data, err := os.ReadFile(dbfilepath)
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
	if err := os.WriteFile(dbfilepath, jsonData, 0644); err != nil {
		return fmt.Errorf("write updated config: %w", err)
	}
	fmt.Printf("File '%s' updated successfully with new embedding.\n", dbfilepath)
	return nil
}

func (s *SQLiteStorage) GetAllFiles(threshold float64, embed []float64) ([]FileRecord, error) {
	// Load all embeddings from DB
	var fileRecords []FileRecord
	if err := s.db.Find(&fileRecords).Error; err != nil {
		log.Printf("Error loading embeddings from DB: %v", err)
	}
	if len(fileRecords) == 0 {
		// nothing to update
		log.Print("GetAllFiles: no embeddings found in DB")
		return nil, nil
	}
	var similarity_arr []float64
	var filemetadata []FileRecord
	for _, fr := range fileRecords {
		similarity, _ := cosineSimilarity(embed, []float64(fr.Embedding))
		if similarity >= threshold {
			similarity_arr = append(similarity_arr, similarity)
			filemetadata = append(filemetadata, fr)
		}
	}
	for i := 0; i < len(similarity_arr)-1; i++ {
		for j := i; j < len(similarity_arr); j++ {
			if similarity_arr[j] > similarity_arr[i] {
				similarity_arr[j], similarity_arr[i] = similarity_arr[i], similarity_arr[j]
				filemetadata[j], filemetadata[i] = filemetadata[i], filemetadata[j]
			}
		}
	}
	return filemetadata, nil
}
