package network

import (
	"encoding/json"
	_ "final/network/RelayFinal/pkg/relay/models"
	"log"
	"os"
)

func FindValueHandler(params map[string]any) []byte {
	log.Printf("params recv to FindValue is: %+v", params)
	// add functionality for checking all params here

	// pseudocode
	// check if the targetnodeid is the same as the current nodeid
	// if yes then find from database and give the new values
	// else find the new values from the routing table

	

	reqJson, err := json.Marshal(params)
	if(err != nil){
		log.Printf("Error while marshalling in FindValueHandler: %+v", err.Error())
	}
	return reqJson
}

func PingHandler(params map[string]any) []byte {
	log.Printf("params recv to PingHandler is: %+v", params)
	// add functionality for checking all params here

	reqJson, err := json.Marshal(params)
	if err != nil {
		log.Printf("error marshalling params in PingHandler: %v", err)
		return nil
	}
	return reqJson
}

func StoreHandler() []byte {
	var resp []byte
	return resp
}

func SendHandler(params map[string]any, bodyBytes []byte) []byte {
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
