package peer

import (
	"final/network/RelayFinal/pkg/network"
	"final/network/RelayFinal/pkg/relay/models"

	"encoding/json"
	"fmt"
	"math/big"
)

var Peer *models.UserPeer

type RelayDist struct {
	relayID string
	dist    *big.Int
}

var NetHandler *network.NetworkHandler

func SetNetworkHandler(handler *network.NetworkHandler) {
    NetHandler = handler
}

func ServeGetReq(paramsBytes []byte) []byte {
	fmt.Println("[DEBUG][ServeGetReq] Received params:", string(paramsBytes))

	var params map[string]any
	err := json.Unmarshal(paramsBytes, &params)
	if err != nil {
		fmt.Println("[ERROR][ServeGetReq] Failed to unmarshal params:", err)
	}
	fmt.Println("[DEBUG][ServeGetReq] Parsed params:", params)


	switch params["route"] {
	case "find_value":
		fmt.Println("[DEBUG][ServeGetReq] Handling route: find_value")
		return NetHandler.FindValueHandler(params)

	case "ping":
		fmt.Println("[DEBUG][ServeGetReq] Handling route: ping")
		return NetHandler.PingHandler(params)

	default:
		fmt.Println("[WARN][ServeGetReq] Unknown route:", params["route"])
	}

	var resp []byte
	return resp
}

func ServePostReq(paramsBytes []byte, bodyBytes []byte) []byte {
	fmt.Println("[DEBUG][ServePostReq] Received params:", string(paramsBytes), " body:", string(bodyBytes))

	var params map[string]any
	err := json.Unmarshal(paramsBytes, &params)
	if err != nil {
		fmt.Println("[ERROR][ServePostReq] Failed to unmarshal params:", err)
	}

	var body map[string]any
	err = json.Unmarshal(bodyBytes, &body)
	if err != nil {
		fmt.Println("[ERROR][ServePostReq] Failed to unmarshal params:", err)
	}

	switch params["route"] {
	case "store":
		fmt.Println("[DEBUG][ServePostReq] Handling route: store")
		return NetHandler.StoreHandler(params, body)
	default:
		fmt.Println("[WARN][ServePostReq] Unknown POST route:", params["route"])
	}

	var resp []byte
	return resp
}
