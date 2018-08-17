package main

import (
	"github.com/georgeteo/sarama-cluster"
	"log"
	"github.com/Shopify/sarama"
)

func main() {
	//sarama.Logger = log.New(os.Stdout, "[Sarama] ", log.LstdFlags)
	config := cluster.NewConfig()
	config.Group.Return.Notifications = true
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	consumer, err := cluster.NewConsumer([]string{"localhost:9092"}, "cg", []string{"test01"}, config)
	if err != nil {
		log.Fatal("unable to start consumer", err)
	}

	for {
		select {
		case msg, ok := <- consumer.Messages():
			if !ok {
				log.Println("ERROR: message channel is closed")
				continue
			}
			log.Printf("INFO: received offset %d from %s-%d\n", msg.Offset, msg.Topic, msg.Partition)
		consumer.MarkOffset(msg, "")
		case err, ok := <- consumer.Errors():
			if !ok {
				log.Println("ERROR: consumer channel is closed")
				continue
			}
			log.Printf("INFO: received error %s from sarama-cluster", err.Error())
		case ntf, ok := <- consumer.Notifications():
			if !ok {
				log.Println("ERROR: notification channel is closed")
				continue
			}
			log.Printf("INFO: received notification %s from sarama-cluster", ntf.Type.String())
		}
	}
}
