package main

import (
	"encoding/json"
	"log"
	"os"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	var recieved_ints []int
	var this_topology map[string][]string
	logfile, err := os.OpenFile("/Users/nicolasgarza/code/dist-sys-challenges/broadcast/debug.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatal("failed to open log file", err)
	}
	defer logfile.Close()
	log.SetOutput(logfile)

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		log.Println("In function broadcast")

		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		if recieved_int, ok := body["message"].(float64); ok {
			this_int := int(recieved_int)
			recieved_ints = append(recieved_ints, this_int)
		} else {
			log.Println("message is not an integer")
		}

		send_data := map[string]any{
			"type":    "broadcast",
			"message": recieved_ints[len(recieved_ints)-1],
		}
		json_data, err := json.Marshal(send_data)
		if err != nil {
			return err
		}

		this_node := n.ID()
		runeSlice := []rune(this_node)
		var firstTwo string
		if len(runeSlice) >= 2 {
			firstTwo = string(runeSlice[:2])
		}

		for _, node_name := range this_topology[firstTwo] {
			if node_name == firstTwo {
				continue
			}
			n.RPC(node_name, json_data, func(_ maelstrom.Message) error { return nil })
		}

		body["type"] = "broadcast_ok"
		delete(body, "message")
		jsonString, err := json.Marshal(body)
		if err != nil {
			log.Printf("Error logging map: %v", err)
		} else {
			log.Printf("Map contents: %s", jsonString)
		}

		return n.Reply(msg, body)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		log.Println("In function read")

		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "read_ok"
		body["messages"] = recieved_ints

		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		log.Println("In function topology")

		var body map[string]interface{}

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		log.Println("--------------------------")
		log.Println(body)
		log.Println("--------------------------")

		topologyInterface, exists := body["topology"]
		if !exists {
			log.Println("topology key does not exist in the message")
		}

		topologyData, ok := topologyInterface.(map[string]interface{})
		if !ok {
			log.Println("incorrect format for topology")
		}

		convertedTopology := make(map[string][]string)
		for key, value := range topologyData {
			if nodeList, ok := value.([]interface{}); ok {
				var stringSlice []string
				for _, node := range nodeList {
					if nodeName, ok := node.(string); ok {
						stringSlice = append(stringSlice, nodeName)
					} else {
						log.Println("node name is not a string")
					}
				}
				convertedTopology[key] = stringSlice
			} else {
				log.Println("nodes are not a slice")
			}
		}

		this_topology = convertedTopology
		body["type"] = "topology_ok"
		delete(body, "topology")

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

}
