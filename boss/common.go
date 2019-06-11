package boss

import (
	"bytes"
	"encoding/gob"
	"log"
	"math/rand"
	"time"

	"github.com/streadway/amqp"
)

func (b *Boss) connector() {
	var (
		rabbitErr *amqp.Error
		err       error
	)

	b.Conn, err = amqp.Dial(b.Address)
	if err != nil {
		log.Fatal("Cant connect to RabbitMQ. Error: ", err)
	}
	b.Conn.NotifyClose(b.CloseError)
	b.setRabbit()
	log.Println("Connected to RabbitMQ")
	retry := &time.Ticker{}
	b.wg.Add(1)
	go func() {
		for {
			select {
			case <-b.ctx.Done():
				log.Println("Rabbit connector has stopped.")
				b.wg.Done()

				return

			case rabbitErr = <-b.CloseError:
				if rabbitErr != nil {
					retry = time.NewTicker(time.Duration(b.Timeout) * time.Second)
				}

			case <-retry.C:
				log.Println("Trying to reconnect to RabbitMQ...")
				b.Conn, err = amqp.Dial(b.Address)
				if err == nil {
					log.Println("Successfully reconnected to RabbitMQ")
					b.CloseError = make(chan *amqp.Error)
					b.Conn.NotifyClose(b.CloseError)

					b.setRabbit()
					//a.ListenRabbit()
					retry.Stop()

					continue
				}
				log.Println("Unable to connect..")
			}
		}
	}()
}

func (b *Boss) setRabbit() {
	var err error

	b.Ch, err = b.Conn.Channel()
	if err != nil {
		log.Println("Error while setting channel. Error: ", err)
	}

	if err = b.Ch.ExchangeDeclare(
		b.Exchange, // name
		"topic",    // type
		false,      // durable
		false,      // auto-deleted
		false,      // internal
		false,      // no-wait
		nil,        // arguments
	); err != nil {
		log.Println("Exchange error: ", err)
	}

	_, err = b.Ch.QueueDeclare(
		b.QueueName, // name
		false,       // durable
		false,       // delete when usused
		false,       // exclusive
		false,       // noWait
		nil,         // arguments
	)
	if err != nil {
		log.Println("Failed to declare a queue. Error: ", err)
	}

	err = b.Ch.QueueBind(
		b.QueueName, // queue name
		b.QueueName, // routing key
		b.Exchange,  // exchange
		false,
		nil)
	if err != nil {
		log.Println("Failed to bind a queue. Error: ", err)
	}

	b.Consume, err = b.Ch.Consume(
		b.QueueName, // queue
		"",          // consumer
		true,        // auto-ack
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // args
	)
	if err != nil {
		log.Println("Failed to register a consumer. Error: ", err)
	}
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
