package boss

import (
	"context"
	"log"
	"sync"

	"github.com/streadway/amqp"
)

//configas turetu eiti cia
func NewWorker(ctx context.Context, address string, timeout int, exchange string, queueName string) *Boss {

	return &Boss{
		Address:   address,
		ctx:       ctx,
		Timeout:   timeout,
		Exchange:  exchange,
		QueueName: queueName,
	}

}

func (b *Boss) StartWork() {
	b.Callbacks = make(map[string]chan amqp.Delivery, 1)

	b.wg = &sync.WaitGroup{}

	b.connector()

	b.listen()

}

func (b *Boss) StopWork() {
	/*if b.Conn != nil {
		b.Conn.Close()
	}*/
	if b.Ch != nil {
		b.Ch.Close()
	}
}

// ListenRabbit function starts listening service that listens for incomming
// RabbitMQ messages
func (b *Boss) listenWork() {
	log.Println("Started Listening service")
	b.wg.Add(1)
	go func() {
		for {
			select {
			case d, ok := <-b.Consume:
				if !ok {
					log.Println("Rabbit channel has been closed")
					b.wg.Done()
					return
				}

				if d.Headers != nil {
					log.Printf("Received a message: %s", d.Headers["action"])
					resp, _ := b.prepareResponse(d)
					b.Tasks[d.Headers["action"].(string)](*resp)
				}
			case <-b.ctx.Done():
				log.Println("Listening service has stopped.")
				b.wg.Done()

				return
			}
		}
	}()
}

func (b *Boss) prepareResponse(cmd amqp.Delivery) (*NodeResponse, error) {
	return &NodeResponse{
		CorrID:  cmd.CorrelationId,
		ReplyTo: cmd.ReplyTo,
		Body:    cmd.Body,
		app:     b,
	}, nil
}

// Publish function sends a message to RabbitMQ
func (n *NodeResponse) Publish(payload []byte, exchange string) {
	err := n.app.Ch.Publish(
		exchange,  // exchange
		n.ReplyTo, // routing key
		false,     // mandatory
		false,     // immediate
		amqp.Publishing{
			Headers: amqp.Table{
				"name": n.app.QueueName,
			},
			ContentType:   "application/gob",
			CorrelationId: n.CorrID,
			Body:          payload,
		})
	if err != nil {
		log.Println("Failed to send message to RabbitMQ. Error: ", err)
	}
}

func (b *Boss) Register(task string, callback func(NodeResponse)) {
	b.Tasks[task] = callback
}
