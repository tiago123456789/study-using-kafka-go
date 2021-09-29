package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	deliveryChan := make(chan kafka.Event)
	producer := NewProducerKafka()
	PushishMessage("Testing1666", "teste", []byte("same_partition"), producer, deliveryChan)
	go DeliveryReport(deliveryChan)
	producer.Flush(2000)
}

func NewProducerKafka() *kafka.Producer {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "study-kafka_kafka_1:9092",
	}

	producer, err := kafka.NewProducer(configMap)
	if err != nil {
		log.Println(err.Error())
	}

	return producer
}

func PushishMessage(message string, topic string, key []byte, producer *kafka.Producer, deliveryChan chan kafka.Event) error {
	messagePublish := &kafka.Message{
		Value:          []byte(message),
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            key,
	}

	err := producer.Produce(messagePublish, deliveryChan)
	if err != nil {
		return err
	}
	return nil
}

func DeliveryReport(deliveryChan chan kafka.Event) {
	for e := range deliveryChan {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				fmt.Println("Erro ao enviar")
			} else {
				fmt.Println("Mensagem enviada:", ev)
			}
		}
	}
}
