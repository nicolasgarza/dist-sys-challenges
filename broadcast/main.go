package broadcast

import (
	"encoding/json"
	"fmt"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	var recieved_ints []int

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

		body["type"] = "generate_ok"
		delete(body, "message")
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
		body := make(map[string]any)
		body["type"] = "topology_ok"
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

}
