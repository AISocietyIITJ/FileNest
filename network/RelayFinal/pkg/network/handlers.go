package network

import (
	// "context"
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
        Found: true,
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
    // Pruned case, no closer nodes found
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
    
    request := models.EmbeddingStoreRequest{}
    json.Unmarshal(params, &request)

    decSourceNodeID, _ := hex.DecodeString(request.SourceNodeID)
    // Store on own DB, base case
    if(request.Depth == 4){
        response.Found = true
        // Store most similar files in D4 DB here. TBA
        // filepath needs to be passed in the request from client side
        err := nh.Kademlia.Node().IndexedFiles.StoreFileEmbedding(decSourceNodeID, request.SourcePeerID, request.FileEmbed, request.FilePath) //need to pass the params here
        if err!=nil{
            log.Printf("[StoreHandler] Could not store the D4 file embed\n")
        }
        serr := nh.Kademlia.Node().IndexedFiles.UpdateD4Config(4, "D4Config.json")
        if serr!=nil{
            log.Printf("[StoreHandler] Config file could not be updated while storing at D4: %v", serr)
        }
        response.Pruned = false
        response.Message = "Stored file on last depth"
        response.QueryEmbed = request.QueryEmbed
        response.FileEmbed = request.FileEmbed
        response.NextNodeID = ""
        response.SourceNodeID = hex.EncodeToString(selfNodeID)
        response.SourcePeerID = nh.Kademlia.Node().RoutingTable().SelfPeerID
        response.Depth = request.Depth+1 //depth=5 now

        respJSON, err := json.Marshal(response)
        if err != nil {
            log.Printf("[StoreHandler] Error marshalling response after found: %v", err)
            return nil
        }
        return respJSON
    }

    // Not depth 4, keep iterating the depths
    // Check D2TV DB for most similar D2TVs indexed
    // also store the node embedding in the respective depth's DB. update the depth json config file too.
    var cmpRes []storage.EmbeddingResult
    var err error
    sourceNodeID, _ := hex.DecodeString(request.SourceNodeID)
    switch request.Depth {
    case 1:
        if serr := nh.Kademlia.Node().D2DB.StoreNodeEmbedding(sourceNodeID, request.SourcePeerID, request.FileEmbed); serr != nil {
            log.Printf("[StoreHandler] Could not store the D1 file embed: %v", serr)
        }
        cmpRes, err = nh.Kademlia.Node().D2DB.FindSimilar(request.QueryEmbed, request.Threshold, request.ResultsCount)
        if err != nil {
            log.Printf("[StoreHandler] Error in FindSimilar D1: %v", err)
            return nil
        }
        err := nh.Kademlia.Node().D2DB.UpdateConfig(1, "D1Config.json")
        if err!=nil{
            log.Printf("[StoreHandler] Config file could not be updated while storing at D2: %v", err)
        }
    case 2:
        if serr := nh.Kademlia.Node().D3DB.StoreNodeEmbedding(sourceNodeID, request.SourcePeerID, request.FileEmbed); serr != nil {
            log.Printf("[StoreHandler] Could not store the D2 file embed: %v", serr)
        }
        cmpRes, err = nh.Kademlia.Node().D3DB.FindSimilar(request.QueryEmbed, request.Threshold, request.ResultsCount)
        if err != nil {
            log.Printf("[StoreHandler] Error in FindSimilar D2: %v", err)
            return nil
        }
        err := nh.Kademlia.Node().D3DB.UpdateConfig(2, "D2Config.json")
        if err!=nil{
            log.Printf("[StoreHandler] Config file could not be updated while storing at D2: %v", err)
        }
    case 3:
        if serr := nh.Kademlia.Node().D4DB.StoreNodeEmbedding(sourceNodeID, request.SourcePeerID, request.FileEmbed); serr != nil {
            log.Printf("[StoreHandler] Could not store the D3 file embed: %v", serr)
        }
        cmpRes, err = nh.Kademlia.Node().D4DB.FindSimilar(request.QueryEmbed, request.Threshold, request.ResultsCount)
        if err != nil {
            log.Printf("[StoreHandler] Error in FindSimilar D3: %v", err)
            return nil
        }
        err := nh.Kademlia.Node().D4DB.UpdateConfig(3, "D3Config.json")
        if err!=nil{
            log.Printf("[StoreHandler] Config file could not be updated while storing at D3: %v", err)
        }
    default:
        log.Printf("[StoreHandler] unexpected depth: %d", request.Depth)
        return nil
    }


    // Pruned case, no closer nodes found
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

    //Not pruned,  Found a suitable cluster
    bestRes := cmpRes[0] // Closest matching embed ki node
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