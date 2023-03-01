package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/segmentio/kafka-go"
)

type KafkaReader interface {
	// Start will connect to kafka and read messages published to the specified topic
	Start() error
	Close() error
}

type kafkaReader struct {
	reader *kafka.Reader
}

func NewKafkaReader(host, topic, group string, partition int) KafkaReader {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{host},
		Topic:     topic,
		Partition: 0,
		GroupID:   group,
	})

	return &kafkaReader{reader}
}

func (k *kafkaReader) Start() error {
	for {
		kMessage, err := k.reader.ReadMessage(context.Background())
		if err != nil {
			return err
		}

		pretty, err := k.beautify(kMessage.Value)
		if err != nil {
			return err
		}
		fmt.Printf("%v\n", string(pretty))
	}
}

func (k kafkaReader) beautify(msg []byte) ([]byte, error) {
	m := make(map[string]any)
	if err := json.Unmarshal(msg, &m); err != nil {
		return nil, err
	}

	pretty, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return nil, err
	}

	return pretty, nil

}

func (k *kafkaReader) Close() error {
	return k.reader.Close()
}
