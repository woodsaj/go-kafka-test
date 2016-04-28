package main

import (
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"log"
	"os"
	"os/signal"
	"time"
)

var brokers = []string{"localhost:9092"}
var topic = "test"

func newProducer(brokerList []string) sarama.SyncProducer {

	// We are looking for strong consistency semantics.
	// Because we don't change the flush settings, sarama will try to produce messages
	// as fast as possible to keep latency low.
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	config.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message

	// On the broker side, you may want to change the following settings to get
	// stronger consistency guarantees:
	// - For your broker, set `unclean.leader.election.enable` to false
	// - For the topic, you could increase `min.insync.replicas`.

	producer, err := sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		log.Fatalln("Failed to start Sarama producer:", err)
	}

	return producer
}

func newConsumer(brokerList []string, groupId string, topics []string) *cluster.Consumer {
	config := cluster.NewConfig()
	//config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Group.Return.Notifications = true
	consumer, err := cluster.NewConsumer(brokerList, groupId, topics, config)
	if err != nil {
		log.Fatalln("failed to start consumer: ", err)
	}
	return consumer
}

func main() {
	log.Println("starting")

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	producer := newProducer(brokers)
	consumer := newConsumer(brokers, "group1", []string{topic})
	stopChan := make(chan struct{})
	defer func() {
		close(stopChan)
		if err := producer.Close(); err != nil {
			log.Println("Failed to shut down producer cleanly", err)
		}
		log.Println("producer closed.")
		if err := consumer.Close(); err != nil {
			log.Println("Failed to shut down consumer cleanly", err)
		}
		log.Println("consumer closed")

	}()

	go produce(producer, stopChan)
	
	// Handle notifications. We only need to handle thses if the condumer config has Group.Return.Notifications set to true.
	// these notifications are emitted everytime a consumer is added/removed from the group resulting in the distribution of
	// partitions between consumers being re-balanced.
	go notifications(consumer)
	
	// sleep for 10seconds. When we start consuming we should see a burst of 10 messsages being consumed,
	// then 1 every second after that.
	time.Sleep(time.Second * 10)
	go consume(consumer)
	
	//wait for CTRL-C
	<-interrupt
}

func produce(producer sarama.SyncProducer, stopChan chan struct{}) {
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case t := <-ticker.C:
			// We are not setting a message key, which means that all messages will
			// be distributed randomly over the different partitions.
			partition, offset, err := producer.SendMessage(&sarama.ProducerMessage{
				Topic: topic,
				Value: sarama.StringEncoder(t.String()),
			})
			if err != nil {
				log.Printf("Error: %s\n", err)
			} else {
				log.Printf("sent: %s, Partition: %d, Offset: %d\n", t.String(), partition, offset)
			}
		case <-stopChan:
			log.Println("shutdown started. stopping producer thread.")
			return
		}
	}
}

func notifications(consumer *cluster.Consumer) {
	// get our notifications Channel
	n := consumer.Notifications()
	for msg := range n {
		if len(msg.Claimed) > 0 {
			for topic, partitions := range msg.Claimed {
				log.Printf("consumer claimed %d partitions on topic: %s\n", len(partitions), topic)
			}
		}
		if len(msg.Released) > 0 {
			for topic, partitions := range msg.Released {
				log.Printf("consumer released %d partitions on topic: %s\n", len(partitions), topic)
			}
		}

		if len(msg.Current) == 0 {
			log.Printf("consumer is no longer consuming from any partitions.")
		} else {
			log.Printf("Current partitions:\n")
			for topic, partitions := range msg.Current {
				log.Printf("%s: %v\n", topic, partitions)
			}
		}
	}
	log.Printf("notification channel closed\n")
}

func consume(consumer *cluster.Consumer) {
	messageChan := consumer.Messages()
	for msg := range messageChan {
		log.Printf("recieved message: Topic %s, Partition: %d, Offset: %d\n", msg.Topic, msg.Partition, msg.Offset)
		log.Printf("message body: %s", string(msg.Value))
		//Acknowledge that we have handled the message.
		consumer.MarkOffset(msg, "")
	}
	log.Println("consumer ended.")
}
