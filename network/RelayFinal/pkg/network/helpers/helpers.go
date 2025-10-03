package helpers

import (
	"context"
	"encoding/json"
	"final/backend/pkg/integration"
	"final/backend/pkg/types"
	genmodels "final/network/RelayFinal/pkg/generalpeer/models"
	"final/network/RelayFinal/pkg/relay/models"
	"final/network/RelayFinal/pkg/relay/peer"
	"fmt"
	"log"
	"math/big"
	"sort"
	"strings"
	"time"
)
type IDs struct {
	PeerID string
	NodeID string // hex string for CLI/logging
}

func SendJSON(p *models.UserPeer, ctx context.Context, pid string, params interface{}, body interface{}) ([]byte, error) {
	paramsJson, _ := json.Marshal(params)
	bodyJson, _ := json.Marshal(body)
	return peer.Send(p, ctx, pid, paramsJson, bodyJson)
}

func ParseBootstrapFlags(nodeidHex, pid string) ([]IDs, error) {
	if nodeidHex == "" || pid == "" {
		return nil, nil
	}
	nodeIDs := strings.Split(nodeidHex, ",")
	peerIDs := strings.Split(pid, ",")
	if len(nodeIDs) != len(peerIDs) {
		return nil, fmt.Errorf("number of node IDs and peer IDs must match")
	}
	ids := make([]IDs, len(nodeIDs))
	for i := range nodeIDs {
		ids[i] = IDs{PeerID: peerIDs[i], NodeID: nodeIDs[i]}
	}
	return ids, nil
}

func SortPeersByDistance(shortlist []types.PeerInfo, targetNodeID []byte) {
	// Sorts the shortlist slice in-place by XOR distance to targetNodeID (ascending)
	sort.Slice(shortlist, func(i, j int) bool {
		distI := xorDistance(shortlist[i].NodeID, targetNodeID)
		distJ := xorDistance(shortlist[j].NodeID, targetNodeID)
		return distI.Cmp(distJ) < 0
	})
}

// xorDistance returns the XOR distance between two node IDs as a big.Int
func xorDistance(a, b []byte) *big.Int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	xor := make([]byte, n)
	for i := 0; i < n; i++ {
		xor[i] = a[i] ^ b[i]
	}
	return new(big.Int).SetBytes(xor)
}

func BuildKademliaFindValueRequest(handler *integration.ComprehensiveKademliaHandler, nodeidBytes []byte, embedding []float64, depth int) (map[string]interface{}, string) {
	inputMessage := genmodels.Message{
		Type:       "find_value",
		QueryEmbed: embedding,
		Depth:      depth,
		IsProcessed: false,
		Found:       false,
	}

	kademliaResponse, err := handler.ProcessEmbeddingRequestWrapper(
		inputMessage.QueryEmbed,
		nodeidBytes,
		inputMessage.Type,
		0.8,
		10,
	)

	outputMessage := genmodels.Message{
		Type:         inputMessage.Type,
		QueryEmbed:   inputMessage.QueryEmbed,
		Depth:        inputMessage.Depth,
		IsProcessed:  err == nil,
		Found:        false,
	}

	if err != nil {
		log.Printf("❌ Kademlia processing failed: %v", err)
	} else {
		outputMessage.Found = kademliaResponse.Found
		log.Printf("✅ Kademlia processed successfully:")
		log.Printf("   Found: %t", kademliaResponse.Found)
		log.Printf("   Query Type: %s", kademliaResponse.QueryType)
		if kademliaResponse.NextNodeID != nil {
			log.Printf("   Next Node: %x", kademliaResponse.NextNodeID[:8])
		}
	}

	return map[string]interface{}{
		"input_message":  inputMessage,
		"output_message": outputMessage,
		"kademlia_used":  true,
		"timestamp":      time.Now().Unix(),
	}, "find_value"
}

func BuildKademliaStoreRequest(handler *integration.ComprehensiveKademliaHandler, nodeidBytes []byte, embedding []float64, depth int) (map[string]interface{}, string) {
	inputMessage := genmodels.Message{
		Type:       "store",
		QueryEmbed: embedding,
		Depth:      depth,
		FileMetadata: genmodels.FileMetadata{
			Name:         "query_document.pdf",
			CreatedAt:    time.Now().Format(time.RFC3339),
			LastModified: time.Now().Format(time.RFC3339),
			FileSize:     512.0,
			UpdatedAt:    time.Now().Format(time.RFC3339),
		},
		IsProcessed: false,
		Found:       false,
	}

	kademliaResponse, err := handler.ProcessEmbeddingRequestWrapper(
		inputMessage.QueryEmbed,
		nodeidBytes,
		inputMessage.Type,
		0.8,
		10,
	)

	outputMessage := genmodels.Message{
		Type:         inputMessage.Type,
		QueryEmbed:   inputMessage.QueryEmbed,
		Depth:        inputMessage.Depth,
		FileMetadata: inputMessage.FileMetadata,
		IsProcessed:  err == nil,
		Found:        false,
	}

	if err != nil {
		log.Printf("❌ Kademlia processing failed: %v", err)
	} else {
		outputMessage.Found = kademliaResponse.Found
		log.Printf("✅ Kademlia processed successfully:")
		log.Printf("   Found: %t", kademliaResponse.Found)
		log.Printf("   Query Type: %s", kademliaResponse.QueryType)
		if kademliaResponse.NextNodeID != nil {
			log.Printf("   Next Node: %x", kademliaResponse.NextNodeID[:8])
		}
	}

	return map[string]interface{}{
		"input_message":  inputMessage,
		"output_message": outputMessage,
		"kademlia_used":  true,
		"timestamp":      time.Now().Unix(),
	}, "store"
}