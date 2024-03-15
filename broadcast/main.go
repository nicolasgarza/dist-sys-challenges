package main

import (
	"encoding/json"
	"fmt"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	var recieved_ints []int
	var this_topology map[string][]string

	n.Handle("broadcast", func(msg maelstrom.Message) error {

		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		if recieved_int, ok := body["message"].(float64); ok {
			this_int := int(recieved_int)
			recieved_ints = append(recieved_ints, this_int)
		} else {
			return fmt.Errorf("message is not an integer")
		}

		send_data := map[string]any{
			"type":    "broadcast",
			"message": recieved_ints[len(recieved_ints)-1],
		}
		json_data, err := json.Marshal(send_data)
		if err != nil {
			return err
		}

		// fmt.Printf("this_topology: %v, this id: %s", this_topology, n.ID())
		this_node := n.ID()
		runeSlice := []rune(this_node)
		var firstTwo string
		if len(runeSlice) >= 2 {
			firstTwo = string(runeSlice[:2])
		}

		// fmt.Printf("This node's neighbors: %s", this_topology[firstTwo])
		for _, node_name := range this_topology[firstTwo] {
			err := n.Send(node_name, json_data)
			if err != nil {
				return err
			}
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

		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "read_ok"
		body["messages"] = recieved_ints

		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		topologyData, ok := body["topology"].(map[string]interface{})
		if !ok {
			return fmt.Errorf("incorrect format for topology")
		}

		convertedTopology := make(map[string][]string)
		for key, value := range topologyData {
			if nodeList, ok := value.([]interface{}); ok {
				var stringSlice []string
				for _, node := range nodeList {
					if nodeName, ok := node.(string); ok {
						stringSlice = append(stringSlice, nodeName)
					} else {
						return fmt.Errorf("node name is not a string")
					}
				}
				convertedTopology[key] = stringSlice
			} else {
				return fmt.Errorf("nodes are not a slice")
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
