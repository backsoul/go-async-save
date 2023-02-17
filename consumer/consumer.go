package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

var productsToPublishQueue []interface{}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

type MessageCatdog struct {
	Products []interface{} `json:"products"`
	Action   string        `json:"action"`
	Adapter  string        `json:"adapter"`
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"golang-queue", // name
		false,          // durable
		false,          // delete when unused
		false,          // exclusive
		false,          // no-wait
		nil,            // arguments
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)

	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)
	channelProcess := make(chan []interface{})
	go func() {
		for d := range msgs {
			go ProcessMessages(d, channelProcess)
		}
	}()
	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
func ProcessMessages(d amqp.Delivery, channelProcess chan []interface{}) {
	var body MessageCatdog
	if err := json.Unmarshal(d.Body, &body); err != nil {
		fmt.Println("Error unmarshalling body: ", err)
	}
	action := body.Action
	switch {
	case action == "publishProduct":
		// valid if body.Adapter == smither or sorl etc..
		fmt.Println(body.Products...)
		productsToPublishQueue = append(productsToPublishQueue, body.Products...)
		jsonString, _ := json.Marshal(productsToPublishQueue)
		log.Printf("publishProduct: %s", jsonString)
	case action == "finishImport":
		go ProcessProducts(channelProcess)
		res := <-channelProcess
		productsToPublishQueue = res
		fmt.Println(productsToPublishQueue)
	}
}
func ProcessProducts(c chan []interface{}) {
	// implement logic for processing products
	productsToPublishQueue = nil
	c <- productsToPublishQueue
}
