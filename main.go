package main

import (
	"encoding/json"
	"log"
	"maelstrom-kafka/internal/fakeafka"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	node := maelstrom.NewNode()

	logStore := fakeafka.NewStore()

	node.Handle("send", func(msg maelstrom.Message) error {
		type sendRequestBody struct {
			maelstrom.MessageBody
			Key string `json:"key"`
			Msg int    `json:"msg"`
		}

		var reqBody *sendRequestBody
		if err := json.Unmarshal(msg.Body, &reqBody); err != nil {
			return err
		}

		offset := logStore.AppendToLog(reqBody.Key, reqBody.Msg)

		// Ack.
		body := make(map[string]any)
		body["type"] = "send_ok"
		body["offset"] = offset
		return node.Reply(msg, body)
	})

	node.Handle("poll", func(msg maelstrom.Message) error {
		type pollRequestBody struct {
			maelstrom.MessageBody
			Offsets map[string]int `json:"offsets"`
		}

		var reqBody *pollRequestBody
		if err := json.Unmarshal(msg.Body, &reqBody); err != nil {
			return err
		}

		// Ack.
		body := make(map[string]any)
		body["type"] = "poll_ok"
		body["msgs"] = logStore.Poll(reqBody.Offsets)
		return node.Reply(msg, body)
	})

	node.Handle("commit_offsets", func(msg maelstrom.Message) error {
		type commitOffsetsRequestBody struct {
			maelstrom.MessageBody
			Offsets map[string]int `json:"offsets"`
		}

		var reqBody *commitOffsetsRequestBody
		if err := json.Unmarshal(msg.Body, &reqBody); err != nil {
			return err
		}

		logStore.CommitOffsets(reqBody.Offsets)

		// Ack.
		body := make(map[string]any)
		body["type"] = "commit_offsets_ok"
		return node.Reply(msg, body)
	})

	node.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		type listCommittedOffsetsRequestBody struct {
			maelstrom.MessageBody
			Keys []string `json:"keys"`
		}

		var reqBody *listCommittedOffsetsRequestBody
		if err := json.Unmarshal(msg.Body, &reqBody); err != nil {
			return err
		}

		offsets := logStore.GetCommittedOffsets(reqBody.Keys)

		// Ack.
		body := make(map[string]any)
		body["type"] = "list_committed_offsets_ok"
		body["offsets"] = offsets
		return node.Reply(msg, body)
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}

}
