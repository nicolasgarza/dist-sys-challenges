package main

import (
	"encoding/json"
	"log"
	"os"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	var (
		recieved_ints []int
		seen          map[int]bool
		mu            sync.Mutex
	)

	seen = make(map[int]bool)
	var this_topology map[string][]string
	logfile, err := os.OpenFile("/Users/nicolasgarza/code/dist-sys-challenges/broadcast/debug.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatal("failed to open log file", err)
	}
	defer logfile.Close()
	log.SetOutput(logfile)

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		if recieved_int, ok := body["message"].(float64); ok {
			mu.Lock()
			this_int := int(recieved_int)
			if !seen[this_int] {
				seen[this_int] = true
				recieved_ints = append(recieved_ints, this_int)
				mu.Unlock()

				go func() {
					send_data := map[string]any{
						"type":    "broadcast",
						"message": int(this_int),
					}
					json_data, err := json.Marshal(send_data)
					if err != nil {
						log.Printf("Error marshaling send_data: %v", err)
						return
					}

					for _, node_name := range this_topology[n.ID()] {
						if node_name != n.ID() {
							retries := 3
							for i := 0; i < retries; i++ {
								if err := n.RPC(node_name, json_data, func(_ maelstrom.Message) error {
									return nil
								}); err != nil {
									log.Printf("Error sending to %s (attempt %d): %v", node_name, i+1, err)
								} else {
									break
								}
							}
						}
					}
				}()
			} else {
				mu.Unlock()
			}
		} else {
			log.Println("message is not an integer")
		}

		body["type"] = "broadcast_ok"
		delete(body, "message")
		return n.Reply(msg, body)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "read_ok"
		mu.Lock()
		body["messages"] = recieved_ints
		mu.Unlock()
		log.Println(recieved_ints)
		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]interface{}

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

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
