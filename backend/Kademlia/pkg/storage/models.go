package storage

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
)

// Custom type for storing float64 slice as JSON in database
type EmbeddingVector []float64

// Implement driver.Valuer interface for storing in database
func (e EmbeddingVector) Value() (driver.Value, error) {
	return json.Marshal(e)
}

// Implement sql.Scanner interface for reading from database
func (e *EmbeddingVector) Scan(value interface{}) error {
	if value == nil {
		*e = nil
		return nil
	}

	bytes, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("cannot scan %T into EmbeddingVector", value)
	}

	return json.Unmarshal(bytes, e)
}

// Database model
type D1TVMap struct {
	ID        uint            `gorm:"primaryKey" json:"id"`
	NodeID    []byte          `gorm:"column:node_id;not null" json:"node_id"`
	PeerID    string          `gorm:"column:peer_id;not null" json:"peer_id"`
	Embedding EmbeddingVector `gorm:"column:embedding;type:text;not null" json:"embedding"`
}

// Database model for indexed files records
type FileRecord struct {
	ID        uint            `gorm:"primaryKey" json:"id"`
	NodeID    []byte          `gorm:"column:node_id;not null" json:"node_id"`
	PeerID    string          `gorm:"column:peer_id;not null" json:"peer_id"`
	Embedding EmbeddingVector `gorm:"column:embedding;type:text;not null" json:"embedding"`
	FilePath  string          `json:"file_name"`
	// Add other metadata fields as needed
}

// Table name
func (D1TVMap) TableName() string {
	return "node_embeddings"
}
