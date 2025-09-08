package storage

import (
    "sort"
    "sync"
    "final/backend/pkg/embedding"
)

type MemoryStorage struct {
    mu   sync.RWMutex
    data map[string][]float64
}

func NewMemoryStorage() *MemoryStorage {
    return &MemoryStorage{
        data: make(map[string][]float64),
    }
}

func (m *MemoryStorage) Store(key []byte, value []float64) error {
    m.mu.Lock()
    defer m.mu.Unlock()
    
    m.data[string(key)] = value
    return nil
}

func (m *MemoryStorage) Get(key []byte) ([]float64, bool) {
    m.mu.RLock()
    defer m.mu.RUnlock()
    
    value, exists := m.data[string(key)]
    return value, exists
}

func (m *MemoryStorage) FindSimilar(queryEmbed []float64, threshold float64, limit int) ([]EmbeddingResult, error) {
    m.mu.RLock()
    defer m.mu.RUnlock()
    
    var results []EmbeddingResult
    
    for keyStr, storedEmbed := range m.data {
        similarity := embedding.CosineSimilarity(queryEmbed, storedEmbed)
        
        if similarity >= threshold {
            results = append(results, EmbeddingResult{
                Key:        []byte(keyStr),
                Embedding:  storedEmbed,
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
    
    return results, nil
}
