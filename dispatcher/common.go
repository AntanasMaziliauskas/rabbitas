package dispatcher

import (
	"bytes"
	"encoding/gob"
	"log"
	"math/rand"
	"time"

	"github.com/streadway/amqp"
)

func setRabbit(conn *amqp.Connection, exchange string, queueName string, broadcastBind string) (*amqp.Channel, <-chan amqp.Delivery) {
	ch, err := conn.Channel()
	if err != nil {
		log.Println("Error while setting channel. Error: ", err)
	}

	if err = ch.ExchangeDeclare(
		exchange, // name
		"topic",  // type
		false,    // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	); err != nil {
		log.Println("Exchange error: ", err)
	}

	_, err = ch.QueueDeclare(
		queueName, // name
		false,     // durable
		false,     // delete when usused
		false,     // exclusive
		false,     // noWait
		nil,       // arguments
	)
	if err != nil {
		log.Println("Failed to declare a queue. Error: ", err)
	}

	err = ch.QueueBind(
		queueName, // queue name
		queueName, // routing key
		exchange,  // exchange
		false,
		nil)
	if err != nil {
		log.Println("Failed to bind a queue. Error: ", err)
	}
	if broadcastBind != "" {
		err = ch.QueueBind(
			queueName, // queue name
			broadcastBind, // routing key
			exchange,  // exchange
			false,
			nil)
		if err != nil {
			log.Println("Failed to bind a queue. Error: ", err)
		}
	}

	consume, err := ch.Consume(
		queueName, // queue
		"",        // consumer
		true,      // auto-ack
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)
	if err != nil {
		log.Println("Failed to register a consumer. Error: ", err)
	}

	return ch, consume
}

//GenerateCorrID function generates random string as correlation ID
func generateCorrID(l int) string {
	rand.Seed(time.Now().UTC().UnixNano())
	bytes := make([]byte, l)
	for i := 0; i < l; i++ {
		bytes[i] = byte(randInt(65, 90))
	}
	return string(bytes)
}

func randInt(min int, max int) int {
	return min + rand.Intn(max-min)
}

//TODO: Issikelia kitur
// GobMarshal is used to marshal struct into gob blob
func GobMarshal(v interface{}) ([]byte, error) {
	b := new(bytes.Buffer)
	err := gob.NewEncoder(b).Encode(v)
	if err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

// GobUnmarshal is used internaly in this package to unmarshal gob blob into
// defined interface.
func GobUnmarshal(data []byte, v interface{}) error {
	b := bytes.NewBuffer(data)
	return gob.NewDecoder(b).Decode(v)
}
