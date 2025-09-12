package network

import (
	// "context"
	"encoding/hex"
	"encoding/json"
	"final/backend/pkg/identity"
	"final/backend/pkg/integration"
	"fmt"

	// "final/network/RelayFinal/pkg/network/helpers"
	// "final/network/RelayFinal/pkg/relay/models"
	_ "final/network/RelayFinal/pkg/relay/models"
	"log"
	"os"
)

// network handler def for dependency injection!
type NetworkHandler struct {
    Kademlia *integration.ComprehensiveKademliaHandler
}

func NewNetworkHandler(kademlia *integration.ComprehensiveKademliaHandler) *NetworkHandler {
    return &NetworkHandler{Kademlia: kademlia}
}

func (nh *NetworkHandler) FindValueHandler(params map[string]any) []byte {
    log.Printf("params recv to FindValue is: %+v", params)

    // !!! TODO: Change TargetNodeID to InitiatorNodeID
    targetNodeIDHex, ok := params["TargetNodeID"].(string)
    if !ok {
        log.Println("Error: TargetNodeID not found or not a string in params")
        return nil
    }

    targetNodeID, err := hex.DecodeString(targetNodeIDHex)
    if err != nil {
        log.Printf("Error decoding TargetNodeID: %v", err)
        return nil
    }

    selfNodeID, err := identity.LoadOrCreateNodeID("");
    if(err != nil){
        log.Printf("Error during selfNodeID: %v", err.Error())
    }

    response := make(map[string]any)

    // Check if the current node is the target
    if string(selfNodeID) == string(targetNodeID) {
        log.Println("This node is the target. Searching for value in local storage.")
        embedding, ok := params["Embedding"].([]any)
        if !ok {
            log.Println("Error: Embedding not found in params for final lookup")
            return nil
        }

        floatEmbedding := make([]float64, len(embedding))
        for i, v := range embedding {
            floatEmbedding[i] = v.(float64)
        }

        similarNodes, err := nh.Kademlia.Node().FindSimilar(floatEmbedding, 0.9, 1)
        if err != nil || len(similarNodes) == 0 {
            log.Printf("Could not find value locally, though this is the target node. Err: %v", err)
            response["Found"] = false
        } else {
            log.Println("Found value in local storage.")
            response["Found"] = true
            response["Value"] = "path/to/your/file.dat" // placeholder
        }
        response["NextPeerID"] = ""

    } else {
        log.Println("This node is not the target. Finding closer peers.")
        closestPeers := nh.Kademlia.Node().RoutingTable().FindClosest(targetNodeID, 1)
        if len(closestPeers) == 0 {
            log.Println("Could not find any closer peer in the routing table.")
            response["Found"] = false
            response["NextPeerID"] = ""
        } else {
            nextPeer := closestPeers[0]
            log.Printf("Found closer peer: %s", nextPeer.PeerID)
            response["Found"] = false
            response["NextPeerID"] = nextPeer.PeerID
            response["NextNodeID"] = hex.EncodeToString(nextPeer.NodeID)
        }
    }

    respJSON, err := json.Marshal(response)
    if err != nil {
        log.Printf("Error while marshalling response in FindValueHandler: %v", err)
        return nil
    }
    return respJSON
}

func (nh *NetworkHandler) PingHandler(params map[string]any) []byte {
	log.Printf("params recv to PingHandler is: %+v", params)
	// add functionality for checking all params here

	reqJson, err := json.Marshal(params)
	if err != nil {
		log.Printf("error marshalling params in PingHandler: %v", err)
		return nil
	}
	return reqJson
}

func (nh *NetworkHandler) StoreHandler(params map[string]any, body map[string]any) []byte {
    log.Printf("[StoreHandler] Received store request with params: %+v", params)

    targetNodeID := params["target_node_id"].(string)

    selfNodeID := nh.Kademlia.Node().NodeID
    response := make(map[string]any)

    // Check if the current node is the target for the store operation
    
    if(params["Found"].(bool)){

        // case where we store value
        if (params["depth"].(int) == 1){ // !!! must be 4 here
            log.Println("[StoreHandler] This node is the target. Storing value.")
    
            // The key is the embedding associated with the data.
            embedding := params["embed"].([]float64)
            if err := nh.Kademlia.Node().StoreNodeEmbedding(selfNodeID, embedding); err != nil {
                log.Printf("[StoreHandler] Error storing value in local storage: %v", err)
                
                response["Stored"] = false
                response["Message"] = "Failed to store value"
            } else {
                log.Printf("[StoreHandler] Successfully stored value: %v\n", embedding)
                
                response["Stored"] = true
                response["Message"] = "Value stored successfully"
            }
        } else { // case where we fwd to next depth
            response["Stored"] = false
            similarNodes, err := nh.Kademlia.Node().FindSimilar(params["embed"].([]float64), params["Threshold"].(float64), 1)
            if err != nil {
                response["Stored"] = false
                response["Message"] = fmt.Sprintf("Error finding similar node: %v", err)
                log.Printf("Error finding similar node: %v", err)
            }
            if len(similarNodes) == 0 {
                response["Stored"] = false
                response["Message"] = fmt.Sprintf("Error finding similar node: %v", err)
                log.Println("Could not find any target node ID above the similarity threshold.")
            }

            similarNode := similarNodes[0]
            response["NextNodeID"] = similarNode.Key
            response["NextPeerID"] = "" // not req. its set in main.go handler
        }
    } else {
        // response["NextPeerID"] = params["SourcePeerID"].(string) // This is the final destination.
        // response["NextNodeID"] = params["SourceNodeID"].(string)

        log.Println("[StoreHandler] This node is not the target. Finding closer peers.")

        // Find the closest peer in the routing table to forward the request to.
        targetNodeIDHex, _ := hex.DecodeString(targetNodeID)
        closestPeers := nh.Kademlia.Node().RoutingTable().FindClosest(targetNodeIDHex, 1)
        if len(closestPeers) == 0 {
            log.Println("[StoreHandler] Could not find any closer peer in the routing table.")
            response["Stored"] = false
            response["NextPeerID"] = ""
        } else {
            nextPeer := closestPeers[0]
            log.Printf("[StoreHandler] Found closer peer: %s", nextPeer.PeerID)
            response["Stored"] = false
            response["NextPeerID"] = nextPeer.PeerID
            response["NextNodeID"] = hex.EncodeToString(nextPeer.NodeID)
        }
    }
            
    respJSON, err := json.Marshal(response)
    if err != nil {
        log.Printf("[StoreHandler] Error marshalling response: %v", err)
        return nil
    }
    return respJSON
}


func (nh *NetworkHandler) SendHandler(params map[string]any, bodyBytes []byte) []byte {
	_, ok := params["Type"].(string)
	if !ok {
		log.Println("invalid params, no type field")
		return nil
	}

	log.Printf("SendHandler called with params: %+v\n", params)

	filename, ok := params["Filename"].(string)
	if !ok || filename == "" {
		log.Println("invalid params, no filename field")
		return nil
	}

	outputPath := "./images/" + filename + ".jpeg"
	err := os.MkdirAll("./images", 0755)
	if err != nil {
		log.Printf("failed to create images directory: %v", err)
		return nil
	}

	err = os.WriteFile(outputPath, bodyBytes, 0644)
	if err != nil {
		log.Printf("failed to write image file: %v", err)
		return nil
	}

	log.Printf("Image saved to %s", outputPath)
	return []byte(`{"status":"success","path":"` + outputPath + `"}`)
}