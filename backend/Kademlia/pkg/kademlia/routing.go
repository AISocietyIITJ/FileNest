package kademlia

import (
	"bytes"
	"final/backend/pkg/helpers"
	"final/backend/pkg/types"
	"fmt"
	"log"
	"sort"
	"sync"
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
	mu         sync.RWMutex // Mutex for concurrent access
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
		// Assuming you have access to a KademliaNode instance to call Ping
		// Replace 'kademliaNode' with the actual instance if available
		// Example:
		// response, err := kademliaNode.Ping(peer.NodeID)
		// if err != nil || !response.Success {
		//     return // Do not add if ping fails
		// }
		// For now, just log the intent
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

// PrintRoutingTable displays the entire routing table in a readable format
func (rt *RoutingTable) PrintRoutingTable() {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	fmt.Println("=== ROUTING TABLE ===")
	fmt.Printf("Self NodeID: %x\n", rt.SelfNodeID)
	fmt.Printf("Self PeerID: %s\n", rt.SelfPeerID)
	fmt.Printf("K (bucket size): %d\n", rt.K)
	fmt.Println()

	totalPeers := 0
	nonEmptyBuckets := 0

	for i, bucket := range rt.Buckets {
		if len(bucket) > 0 {
			nonEmptyBuckets++
			totalPeers += len(bucket)
			fmt.Printf("Bucket %d (%d peers):\n", i, len(bucket))

			for j, peer := range bucket {
				fmt.Printf("  [%d] NodeID: %x\n", j, peer.NodeID)
				fmt.Printf("      PeerID: %s\n", peer.PeerID)
				fmt.Printf("      Distance: %x\n", rt.calculateDistance(peer.NodeID))
				fmt.Println()
			}
		}
	}

	fmt.Printf("Summary: %d peers across %d buckets (out of %d total buckets)\n",
		totalPeers, nonEmptyBuckets, len(rt.Buckets))
	fmt.Println("========================")
}

// PrintBucket displays a specific bucket's contents
func (rt *RoutingTable) PrintBucket(bucketIndex int) {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	if bucketIndex < 0 || bucketIndex >= len(rt.Buckets) {
		fmt.Printf("Invalid bucket index: %d (valid range: 0-%d)\n", bucketIndex, len(rt.Buckets)-1)
		return
	}

	bucket := rt.Buckets[bucketIndex]
	fmt.Printf("=== BUCKET %d ===\n", bucketIndex)

	if len(bucket) == 0 {
		fmt.Println("Empty bucket")
		fmt.Println("================")
		return
	}

	fmt.Printf("Contains %d peers:\n", len(bucket))
	for i, peer := range bucket {
		fmt.Printf("[%d] NodeID: %x\n", i, peer.NodeID)
		fmt.Printf("    PeerID: %s\n", peer.PeerID)
		fmt.Printf("    Distance: %x\n", rt.calculateDistance(peer.NodeID))
		fmt.Println()
	}
	fmt.Println("================")
}

// PrintPeerInfo displays detailed information about all peers
func (rt *RoutingTable) PrintPeerInfo() {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	fmt.Println("=== PEER INFORMATION ===")

	allPeers := rt.GetAllPeers()
	if len(allPeers) == 0 {
		fmt.Println("No peers in routing table")
		fmt.Println("========================")
		return
	}

	for i, peer := range allPeers {
		fmt.Printf("Peer %d:\n", i+1)
		fmt.Printf("  NodeID: %x\n", peer.NodeID)
		fmt.Printf("  PeerID: %s\n", peer.PeerID)
		fmt.Printf("  Distance from self: %x\n", rt.calculateDistance(peer.NodeID))
		fmt.Printf("  Bucket: %d\n", helpers.BucketIndex(rt.SelfNodeID, peer.NodeID))
		fmt.Println()
	}

	fmt.Printf("Total peers: %d\n", len(allPeers))
	fmt.Println("========================")
}

// GetAllPeers returns all peers from all buckets
func (rt *RoutingTable) GetAllPeers() []types.PeerInfo {
	var allPeers []types.PeerInfo
	for _, bucket := range rt.Buckets {
		allPeers = append(allPeers, bucket...)
	}
	return allPeers
}

// Helper method to calculate XOR distance
func (rt *RoutingTable) calculateDistance(targetID []byte) []byte {
	return helpers.XORDistance(rt.SelfNodeID, targetID).Bytes()
}

// PrintRoutingTableSummary displays a compact summary
func (rt *RoutingTable) PrintRoutingTableSummary() {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	fmt.Println("=== ROUTING TABLE SUMMARY ===")
	fmt.Printf("Self: %x (%s)\n", rt.SelfNodeID[:8], rt.SelfPeerID) // Show first 8 bytes

	totalPeers := 0
	bucketStats := make(map[int]int)

	for i, bucket := range rt.Buckets {
		if len(bucket) > 0 {
			bucketStats[i] = len(bucket)
			totalPeers += len(bucket)
		}
	}

	fmt.Printf("Total Peers: %d\n", totalPeers)
	fmt.Printf("Active Buckets: %d/%d\n", len(bucketStats), len(rt.Buckets))

	if len(bucketStats) > 0 {
		fmt.Println("Bucket Distribution:")
		for bucket, count := range bucketStats {
			fmt.Printf("  Bucket %d: %d peers\n", bucket, count)
		}
	}

	fmt.Println("=============================")
}
