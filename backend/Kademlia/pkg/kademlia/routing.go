package kademlia

import (
	"bytes"
	"final/backend/pkg/helpers"
	"final/backend/pkg/types"
	"fmt"
	"log"
	"sort"
)
const (
    NodeIDLength = 20   // 160 bits = 20 bytes
    NodeIDBits   = 160  // Total bits in Kademlia key space
)
type RoutingTable struct {
	SelfNodeID []byte // Persistent unique node ID (used for XOR)
	SelfPeerID string // Ephemeral PeerID from libp2p
	Buckets    [][]types.PeerInfo
	K          int          // Max bucket size
	// mu         sync.RWMutex // Mutex for concurrent access
}

// NewRoutingTable initializes a new routing table
func NewRoutingTable(selfNodeID []byte, selfPeerID string, k int) *RoutingTable {
	log.Printf("NewRoutingTable: len NID: %v", len(selfNodeID)) // 20 hai idhar

	if len(selfNodeID) != NodeIDLength {
        panic(fmt.Sprintf("nodeID must be %d bytes, got %d", NodeIDLength, len(selfNodeID)))
    }
	
	numBuckets := len(selfNodeID) * 8
	buckets := make([][]types.PeerInfo, numBuckets)
	return &RoutingTable{
		SelfNodeID: selfNodeID,
		SelfPeerID: selfPeerID,
		Buckets:    buckets,
		K:          k,
	}
}

// Update adds or refreshes a peer in the correct bucket and pings the peer before adding
func (rt *RoutingTable) Update(peer types.PeerInfo) {
	if bytes.Equal(rt.SelfNodeID, peer.NodeID) {
		return // don't add own peer
	}

	// Ping the peer before adding to the routing table
	if rt.SelfPeerID != "" && peer.PeerID != "" {
		fmt.Printf("Pinging peer before adding: %s\n", peer.PeerID)
	}

	log.Printf("routing.go: selfNID: %v peer NID: %v",len(rt.SelfNodeID), len(peer.NodeID) )
	bucketIndex := helpers.BucketIndex(rt.SelfNodeID, peer.NodeID)
	if bucketIndex < 0 || bucketIndex >= len(rt.Buckets) {
		return // invalid bucket index
	}

	bucket := rt.Buckets[bucketIndex]

	// If peer already exists, move to front
	for i, p := range bucket {
		if bytes.Equal(p.NodeID, peer.NodeID) {
			bucket = append([]types.PeerInfo{p}, append(bucket[:i], bucket[i+1:]...)...)
			rt.Buckets[bucketIndex] = bucket
			return
		}
	}

	// If not full, add to front
	if len(bucket) < rt.K {
		bucket = append([]types.PeerInfo{peer}, bucket...)

		rt.Buckets[bucketIndex] = bucket
		return
	} else {
		// If full, remove last and insert at front
		bucket = append([]types.PeerInfo{peer}, bucket[:rt.K-1]...)
		rt.Buckets[bucketIndex] = bucket
	}

}

// FindClosest returns count closest peers to target NodeID
func (rt *RoutingTable) FindClosest(targetNodeID []byte, count int) []types.PeerInfo {
	var allPeers []types.PeerInfo
	for _, bucket := range rt.Buckets {
		allPeers = append(allPeers, bucket...)
	}

	// Sort by XOR distance of NodeIDs
	sort.Slice(allPeers, func(i, j int) bool {
		distI := helpers.XORDistance(targetNodeID, allPeers[i].NodeID)
		distJ := helpers.XORDistance(targetNodeID, allPeers[j].NodeID)
		return distI.Cmp(distJ) < 0
	})

	if count > len(allPeers) {
		count = len(allPeers)
	}

	return allPeers[:count]
}

// Remove deletes a peer from its bucket
func (rt *RoutingTable) Remove(nodeID []byte) {
	bucketIndex := helpers.BucketIndex(rt.SelfNodeID, nodeID)
	if bucketIndex < 0 || bucketIndex >= len(rt.Buckets) {
		return
	}

	bucket := rt.Buckets[bucketIndex]
	for i, p := range bucket {
		if bytes.Equal(p.NodeID, nodeID) {
			bucket = append(bucket[:i], bucket[i+1:]...)
			rt.Buckets[bucketIndex] = bucket
			return
		}
	}
}

// GetNodes returns all peers currently in the routing table
func (rt *RoutingTable) GetNodes() []types.PeerInfo {
	var allPeers []types.PeerInfo
	seen := make(map[string]bool)

	for _, bucket := range rt.Buckets {
		for _, peer := range bucket {
			key := string(peer.NodeID) // using NodeID as unique key
			if !seen[key] {
				seen[key] = true
				allPeers = append(allPeers, peer)
			}
		}
	}

	return allPeers
}

// GetAllPeers returns all peers from all buckets
func (rt *RoutingTable) GetAllPeers() []types.PeerInfo {
	var allPeers []types.PeerInfo
	for _, bucket := range rt.Buckets {
		allPeers = append(allPeers, bucket...)
	}
	return allPeers
}
