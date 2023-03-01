package main

import (
	"errors"
	"flag"
	"log"
)

var (
	format    = flag.String("fmt", "text", "It tells how we should format the output")
	host      = flag.String("host", "localhost:9092", "The kafka address")
	topic     = flag.String("topic", "", "The topic to subscribe to")
	group     = flag.String("group", "console", "The consumer group")
	partition = flag.Int("partition", 0, "The topic partition to consume")
)

func main() {
	flag.Parse()
	if len(*topic) == 0 {
		log.Fatal(errors.New("You should provide a topic"))
	}

	kafkareader := NewKafkaReader(*host, *topic, *group, *partition)
	defer kafkareader.Close()

	if err := kafkareader.Start(); err != nil {
		log.Fatal(err)
	}
}
