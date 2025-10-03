package network

import (
	// "context"
	"encoding/hex"
	"encoding/json"
	"final/backend/pkg/identity"
	"final/backend/pkg/integration"

	// "final/network/RelayFinal/pkg/network/helpers"
	// "final/network/RelayFinal/pkg/relay/models"
	"final/network/RelayFinal/pkg/relay/models"
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

func (nh *NetworkHandler) FindNodeHandler(params map[string]any) []byte {
    log.Printf("[FindNodeHandler] Received find_node request with params: %+v", params)

    // Unmarshal the generic map into a structured request for type safety.
    jsonBytes, err := json.Marshal(params)
    if err != nil {
        log.Printf("[FindNodeHandler] Error marshalling params: %v", err)
        return nil
    }

    var req models.FindNodeRequest
    if err := json.Unmarshal(jsonBytes, &req); err != nil {
        log.Printf("[FindNodeHandler] Error unmarshalling request: %v", err)
        return nil
    }

    // Find k closest peers in own RT
    closestPeers := nh.Kademlia.Node().RoutingTable().FindClosest(req.TargetID, 20)
    if len(closestPeers) == 0 {
        log.Println("[FindNodeHandler] Could not find any peers in the routing table.")
    }

    log.Printf("[FindNodeHandler] Found %d closer peers.", len(closestPeers))

    // Marshal the list of peers and send it back as the response.
    respJSON, err := json.Marshal(closestPeers)
    if err != nil {
        log.Printf("[FindNodeHandler] Error marshalling response: %v", err)
        return nil
    }
    return respJSON
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
            response["found"] = false
        } else {
            log.Println("found value in local storage.")
            response["found"] = true
            response["Value"] = "path/to/your/file.dat" // placeholder
        }
        response["NextPeerID"] = ""

    } else {
        log.Println("This node is not the target. Finding closer peers.")
        closestPeers := nh.Kademlia.Node().RoutingTable().FindClosest(targetNodeID, 1)
        if len(closestPeers) == 0 {
            log.Println("Could not find any closer peer in the routing table.")
            response["found"] = false
            response["NextPeerID"] = ""
        } else {
            nextPeer := closestPeers[0]
            log.Printf("found closer peer: %s", nextPeer.PeerID)
            response["found"] = false
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

// type EmbeddingSearchRequest struct {
// 	Route        string    `json:"route"`
// 	SourceNodeID string    `json:"source_node_id"`
// 	SourcePeerID string    `json:"source_peer_id"`
// 	NextNodeID string	   `json:"next_node_id"`
// 	NextPeerID string      `json:"next_peer_id"`
// 	ReceiverPeerID string `json:"receiver_peer_id"`
// 	QueryEmbed   []float64 `json:"embed"`
// 	Depth        int       `json:"prev_depth"`
// 	Type         string    `json:"type"`
// 	Threshold    float64   `json:"threshold"`
// 	ResultsCount int       `json:"results_count"`
// 	TargetNodeID string    `json:"target_node_id"`
// 	Found bool `json:"found"`
// }


func (nh *NetworkHandler) StoreHandler(params []byte, body map[string]any) []byte {
    log.Printf("[StoreHandler] Received store request with params: %+v", params)
    response := models.EmbeddingSearchResponse{}
    selfNodeID := nh.Kademlia.Node().NodeID
    
    request := models.EmbeddingSearchRequest{}
    json.Unmarshal(params, &request)
    
    nextNodeID := request.NextNodeID
    nextPeerID := request.NextPeerID
    thres := request.Threshold
    // Check D2TV DB for most similar D2TVs indexed
    cmpRes, err := nh.Kademlia.Node().FindSimilar(request.QueryEmbed, thres, 1)
    if(err != nil){
        log.Printf("[StoreHandler] Error in FindSimilar: %v", err)
        return nil
    }

    if(len(cmpRes) == 0){
        response.Message = "Pruning this lookup, no suitable cluster found."
        response.Pruned = true
        response.Found = false

        respJSON, err := json.Marshal(response)
        if err != nil {
            log.Printf("[StoreHandler] Error marshalling response after pruning: %v", err)
            return nil
        }
        return respJSON
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