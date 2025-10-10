package network

import (
	// "context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"final/backend/pkg/integration"
	"final/backend/pkg/storage"
	"final/backend/pkg/types"
	"fmt"
	"time"

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
    strTargetNodeID, _ := hex.DecodeString(req.TargetNodeID)

    //this node is the target. return own pid, nid
    if(req.TargetNodeID == hex.EncodeToString(nh.Kademlia.Node().NodeID)){
        var ownInfo []types.PeerInfo
        ownInfo = append(ownInfo, types.PeerInfo{NodeID: nh.Kademlia.Node().NodeID, PeerID: nh.Kademlia.Node().PeerID})
        response := models.FindNodeResponse{
            SenderNodeID: hex.EncodeToString(nh.Kademlia.Node().NodeID),
            SenderPeerID: nh.Kademlia.Node().PeerID,
            ClosestNodes: ownInfo,
            Found: true,
        }
        respJSON, err := json.Marshal(response)
        if err != nil {
            log.Printf("[FindNodeHandler] Error marshalling response: %v", err)
            return nil
        }
        return respJSON
    }
    closestPeers := nh.Kademlia.Node().RoutingTable().FindClosest(strTargetNodeID, 20)
    if len(closestPeers) == 0 {
        log.Println("[FindNodeHandler] Could not find any peers in the routing table.")

        response := models.FindNodeResponse{
            SenderNodeID: hex.EncodeToString(nh.Kademlia.Node().NodeID),
            SenderPeerID: nh.Kademlia.Node().PeerID,
            ClosestNodes: nil,
            Timestamp: time.Now().Unix(),
            Found: false,
        }

        respJSON, err := json.Marshal(response)
        if err != nil {
            log.Printf("[FindNodeHandler] Error marshalling response: %v", err)
            return nil
        }
        return respJSON 
    }
    log.Printf("[FindNodeHandler] Found %d closer peers.", len(closestPeers))

    
    response := models.FindNodeResponse{
        SenderNodeID: hex.EncodeToString(nh.Kademlia.Node().NodeID),
        SenderPeerID: nh.Kademlia.Node().PeerID,
        ClosestNodes: closestPeers,
        Found: false,
    }
    respJSON, err := json.Marshal(response)
    if err != nil {
        log.Printf("[FindNodeHandler] Error marshalling response: %v", err)
        return nil
    }
    return respJSON
}

func (nh *NetworkHandler) FindValueHandler(params map[string]any) []byte {
    log.Printf("params recv to FindValue is: %+v", params)

    paramsBytes, _ := json.Marshal(params)
    var request models.EmbeddingSearchRequest
    json.Unmarshal(paramsBytes, &request)

    targetNodeID := request.TargetNodeID
    selfNodeID := hex.EncodeToString(nh.Kademlia.Node().NodeID)
    
    //Req sent to wrong node, add error params to response here instead of nil. 
    if(targetNodeID != selfNodeID){
        return nil
    }

    // Depth=4, return indexed files
    if request.Depth == 4 {
        log.Println("[FindValueHandler] Reached depth 4, returning all indexed files")

        files, err := nh.Kademlia.Node().IndexedFiles.GetAllFiles(request.Threshold, request.QueryEmbed) // !!! complete this fn
        if err != nil {
            errString := fmt.Sprintf("[FindValueHandler] Error retrieving files from D4 storage: %v", err)
            log.Printf("%s", errString)
            response := models.EmbeddingSearchResponse{
                Found:        false,
                Message:      errString,
                Depth:        request.Depth + 1,
                FileEmbeds:   nil,
                NextNodeID:   "",
                Pruned:       true,
                SourceNodeID: selfNodeID,
                SourcePeerID: nh.Kademlia.Node().PeerID,
            }

            respJSON, err := json.Marshal(response)
            if err != nil {
                log.Printf("Error while marshalling response in FindValueHandler: %v", err)
                return nil
            }
            return respJSON
        }
        // list of all file embeds
        var allFiles [][]float64
        if len(files) > 0 {
            for _,file := range files{
                allFiles = append(allFiles, file.Embedding)
            }
        }
        
        response := models.EmbeddingSearchResponse{
            Found:        true,
            Message:      fmt.Sprintf("Found %d files at depth 4", len(files)),
            Depth:        request.Depth + 1,
            FileEmbeds:   allFiles,
            NextNodeID:   "",
            Pruned:       false,
            SourceNodeID: selfNodeID,
            SourcePeerID: nh.Kademlia.Node().PeerID,
        }
        respJSON, err := json.Marshal(response)
        if err != nil {
            log.Printf("Error while marshalling response in FindValueHandler: %v", err)
            return nil
        }
        return respJSON
    }

    // Not depth 4, keep iterating the depths
    // Check D2TV DB for most similar D2TVs indexed

    var cmpRes []storage.EmbeddingResult
    var err error
    switch request.Depth {
    case 1:
        //DnDB stores nth depth nodes
        cmpRes, err = nh.Kademlia.Node().D2DB.FindSimilar(request.QueryEmbed, request.Threshold, request.ResultsCount)
        if err != nil {
            log.Printf("[FindValueHandler] Error in FindSimilar D1: %v", err)
            return nil
        }
    case 2:
        cmpRes, err = nh.Kademlia.Node().D3DB.FindSimilar(request.QueryEmbed, request.Threshold, request.ResultsCount)
        if err != nil {
            log.Printf("[FindValueHandler] Error in FindSimilar D2: %v", err)
            return nil
        }
    case 3:
        cmpRes, err = nh.Kademlia.Node().D4DB.FindSimilar(request.QueryEmbed, request.Threshold, request.ResultsCount)
        if err != nil {
            log.Printf("[FindValueHandler] Error in FindSimilar D3: %v", err)
            return nil
        }
    default:
        log.Printf("[FindValueHandler] unexpected depth: %d", request.Depth)
        return nil
    }

    var response models.EmbeddingSearchResponse
    // Pruned case, no closer nodes found for query
    if(len(cmpRes) == 0){
        response.Message = "Pruning this lookup, no suitable cluster found."
        response.Pruned = true
        response.Found = false

        respJSON, err := json.Marshal(response)
        if err != nil {
            log.Printf("[FindValueHandler] Error marshalling response after pruning: %v", err)
            return nil
        }
        return respJSON
    }
    
    //Not pruned,  Found a suitable cluster
    bestRes := cmpRes[0] // Closest matching embed ki node
    response.Depth = request.Depth+1
    response.Message = fmt.Sprintf("Found next Node on depth %d", response.Depth)
    response.SourceNodeID = selfNodeID
    response.SourcePeerID = nh.Kademlia.Node().RoutingTable().SelfPeerID
    response.NextNodeID = hex.EncodeToString(bestRes.NodeID)
    response.Found = false
    response.Pruned= false

    respJSON, err := json.Marshal(response)
    if err != nil {
        log.Printf("[FindValueHandler] Error marshalling response: %v", err)
        return nil
    }
    return respJSON

}

func (nh *NetworkHandler) PingHandler(params map[string]any) []byte {
	log.Printf("params recv to PingHandler is: %+v", params)
    paramsBytes, _ := json.Marshal(params)
    
    var req models.PingRequest
    json.Unmarshal(paramsBytes, &req)
    senderInfo := types.PeerInfo{NodeID: req.SenderNodeID, PeerID: req.SenderPeerID}
    nh.Kademlia.AddPeerToRoutingTable(senderInfo)

    resp := models.PingResponse{
        SenderNodeID: nh.Kademlia.Node().NodeID,
        SenderPeerID: nh.Kademlia.Node().PeerID,
        Timestamp: time.Now().Unix(),
        Success: true,
    }

    respJson, _ := json.Marshal(resp)
    return respJson
}

func (nh *NetworkHandler) StoreHandler(params []byte, body map[string]any) []byte {
    log.Printf("[StoreHandler] Received store request with params: %+v", params)
    response := models.EmbeddingStoreResponse{}
    selfNodeID := nh.Kademlia.Node().NodeID
    
    // Parse the incoming request parameters into a structured format
    request := models.EmbeddingStoreRequest{}
    json.Unmarshal(params, &request)

    // Decode the source node ID from hex string to bytes for storage operations
    decSourceNodeID, _ := hex.DecodeString(request.SourceNodeID)
    
    // BASE CASE: Depth 4 - Store the file embedding at the final depth
    if(request.Depth == 4){
        log.Println("[StoreHandler] Reached depth 4, storing file embedding in IndexedFiles")
        response.Found = true

        // Store the actual file embedding in the D4 database (IndexedFiles)
        // This is the final storage location for file embeddings
        err := nh.Kademlia.Node().IndexedFiles.StoreFileEmbedding(decSourceNodeID, request.SourcePeerID, request.FileEmbed, request.FilePath)
        if err!=nil{
            log.Printf("[StoreHandler] Could not store the D4 file embed\n")
        }
        
        // Update the configuration file to reflect the new storage
        serr := nh.Kademlia.Node().IndexedFiles.UpdateD4Config(4, "D4Config.json")
        if serr!=nil{
            log.Printf("[StoreHandler] Config file could not be updated while storing at D4: %v", serr)
        }
        
        // Prepare success response for depth 4 storage
        response.Pruned = false
        response.Message = "Stored file on last depth"
        response.QueryEmbed = request.QueryEmbed
        response.FileEmbed = request.FileEmbed
        response.NextNodeID = ""
        response.SourceNodeID = hex.EncodeToString(selfNodeID)
        response.SourcePeerID = nh.Kademlia.Node().RoutingTable().SelfPeerID
        response.Depth = request.Depth+1 // Increment to depth 5 to indicate completion

        respJSON, err := json.Marshal(response)
        if err != nil {
            log.Printf("[StoreHandler] Error marshalling response after found: %v", err)
            return nil
        }
        return respJSON
    }

    // RECURSIVE CASE: Depths 1-3 - Find the next node to forward the request to
    log.Printf("[StoreHandler] At depth %d, searching for similar embeddings to find next hop", request.Depth)
    
    var cmpRes []storage.EmbeddingResult
    var err error
    
    // Search for similar embeddings in the appropriate depth database
    // Each depth has its own database storing node embeddings for that level
    switch request.Depth {
    case 1:
        // Search D2DB for nodes that have similar embeddings at depth 2
        cmpRes, err = nh.Kademlia.Node().D2DB.FindSimilar(request.QueryEmbed, request.Threshold, request.ResultsCount)
        if err != nil {
            log.Printf("[StoreHandler] Error in FindSimilar D2DB: %v", err)
            return nil
        }
        log.Println("[StoreHandler] Searched D2DB for similar embeddings")
        
    case 2:
        // Search D3DB for nodes that have similar embeddings at depth 3
        cmpRes, err = nh.Kademlia.Node().D3DB.FindSimilar(request.QueryEmbed, request.Threshold, request.ResultsCount)
        if err != nil {
            log.Printf("[StoreHandler] Error in FindSimilar D3DB: %v", err)
            return nil
        }
        log.Println("[StoreHandler] Searched D3DB for similar embeddings")
        
    case 3:
        // Search D4DB for nodes that have similar embeddings at depth 4
        cmpRes, err = nh.Kademlia.Node().D4DB.FindSimilar(request.QueryEmbed, request.Threshold, request.ResultsCount)
        if err != nil {
            log.Printf("[StoreHandler] Error in FindSimilar D4DB: %v", err)
            return nil
        }
        log.Println("[StoreHandler] Searched D4DB for similar embeddings")
        
    default:
        log.Printf("[StoreHandler] unexpected depth: %d", request.Depth)
        return nil
    }

    // CLUSTER CREATION CASE: No similar embeddings found - create a new cluster
    if(len(cmpRes) == 0){
        log.Printf("[StoreHandler] No similar embeddings found at depth %d, creating new cluster", request.Depth)
        
        // Hash the query embedding to determine which node should store it
        h := sha256.New()
        embedBytes, _ := json.Marshal(request.QueryEmbed)
        h.Write(embedBytes)
        
        // Find the closest node in the routing table based on the hash
        // This creates a deterministic mapping from embeddings to nodes
        closestNodes := nh.Kademlia.Node().RoutingTable().FindClosest(embedBytes, 1)
        closestNode := closestNodes[0]
        log.Printf("[StoreHandler] Selected closest node %s to store new cluster", hex.EncodeToString(closestNode.NodeID))
        
        // Store the query embedding as a new cluster center in the appropriate depth database
        switch request.Depth {
        case 1:
            // Store the embedding in D2DB to indicate this node handles depth 2 queries for this embedding cluster
            err = nh.Kademlia.Node().D2DB.StoreNodeEmbedding(closestNode.NodeID, closestNode.PeerID, request.QueryEmbed)
            if err != nil {
                log.Printf("[StoreHandler] Error storing in D2DB: %v", err)
                return nil
            }
            nh.Kademlia.Node().D2DB.UpdateConfig(1, "D1Config.json")
            log.Println("[StoreHandler] Created new cluster in D2DB")

        case 2:
            // Store the embedding in D3DB to indicate this node handles depth 3 queries for this embedding cluster
            err = nh.Kademlia.Node().D3DB.StoreNodeEmbedding(closestNode.NodeID, closestNode.PeerID, request.QueryEmbed)
            if err != nil {
                log.Printf("[StoreHandler] Error storing in D3DB: %v", err)
                return nil   
            }
            nh.Kademlia.Node().D3DB.UpdateConfig(1, "D1Config.json")
            log.Println("[StoreHandler] Created new cluster in D3DB")
        
        case 3:
            // Store the embedding in D4DB to indicate this node handles depth 4 queries for this embedding cluster
            err = nh.Kademlia.Node().D4DB.StoreNodeEmbedding(closestNode.NodeID, closestNode.PeerID, request.QueryEmbed)
            if err != nil {
                log.Printf("[StoreHandler] Error storing in D4DB: %v", err)
                return nil
            }
            nh.Kademlia.Node().D4DB.UpdateConfig(1, "D1Config.json")
            log.Println("[StoreHandler] Created new cluster in D4DB")

        default:
            log.Printf("[StoreHandler] unexpected depth: %d", request.Depth)
            return nil
        }

        // Prepare response indicating a new cluster was created and routing to the chosen node
        response.Message = "Stored embed in closest hashed node."
        response.Pruned = false
        response.Found = false
        response.NextNodeID = hex.EncodeToString(closestNode.NodeID)
        response.Depth = request.Depth + 1
        response.FileEmbed = request.FileEmbed
        response.SourceNodeID = hex.EncodeToString(nh.Kademlia.Node().NodeID)
        response.SourcePeerID = nh.Kademlia.Node().PeerID

        respJSON, err := json.Marshal(response)
        if err != nil {
            log.Printf("[StoreHandler] Error marshalling response after cluster creation: %v", err)
            return nil
        }
        return respJSON
    }

    // ROUTING CASE: Similar embeddings found - route to the most similar cluster
    log.Printf("[StoreHandler] Found %d similar embeddings, routing to best match", len(cmpRes))
    
    // Select the node with the most similar embedding as the next hop
    bestRes := cmpRes[0] // Results are sorted by similarity, so first is best match
    log.Printf("[StoreHandler] Routing to node %s with best embedding match", hex.EncodeToString(bestRes.NodeID))
    
    // Prepare response to route the request to the next depth
    response.Depth = request.Depth+1
    response.Message = fmt.Sprintf("Found next Node on depth %d", response.Depth)
    response.QueryEmbed = request.QueryEmbed
    response.SourceNodeID = hex.EncodeToString(selfNodeID)
    response.SourcePeerID = nh.Kademlia.Node().RoutingTable().SelfPeerID
    response.NextNodeID = hex.EncodeToString(bestRes.NodeID)
    response.Found = false
    response.Pruned= false

    respJSON, err := json.Marshal(response)
    if err != nil {
        log.Printf("[StoreHandler] Error marshalling routing response: %v", err)
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