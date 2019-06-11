package boss

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

type Boss struct {
	Address    string
	Conn       *amqp.Connection
	CloseError chan *amqp.Error
	Timeout    int
	Ch         *amqp.Channel
	Exchange   string
	QueueName  string
	Consume    <-chan amqp.Delivery
	Callbacks  map[string]chan amqp.Delivery
	//Tik workeriui
	Tasks map[string]func(NodeResponse)

	wg     *sync.WaitGroup
	cancel context.CancelFunc
	ctx    context.Context
}

type NodeResponse struct {
	CorrID  string
	ReplyTo string
	Body    []byte
	app     *Boss
}

//configas turetu eiti cia
func NewBoss(ctx context.Context, address string, timeout int, exchange string, queueName string) *Boss {

	return &Boss{
		Address:   address,
		ctx:       ctx,
		Timeout:   timeout,
		Exchange:  exchange,
		QueueName: queueName,
	}

}

func (b *Boss) Start() {
	b.Callbacks = make(map[string]chan amqp.Delivery, 1)

	b.wg = &sync.WaitGroup{}

	b.connector()

	b.listen()

}

func (b *Boss) Stop() {
	//TODO: sitoj vietoj race condition ar kas gaunasi? Panic, kad channel is closed...
	/*if b.Conn != nil {
		b.Conn.Close()
	}*/
	if b.Ch != nil {
		b.Ch.Close()
	}

	b.wg.Wait()
}

// ListenRabbit function start listening for incomming messages from RabbitMQ
func (b *Boss) listen() {
	b.wg.Add(1)
	go func() {
		log.Println("Listening to Rabbit for incomming messages")
		for {
			select {
			case d, ok := <-b.Consume:
				if !ok {
					log.Println("RabbitMQ channel has been closed")
					b.wg.Done()
					return
				}
				log.Printf("Server received reply. %#v", d.Headers)
				if _, exists := b.Callbacks[d.CorrelationId]; exists {
					go func() {
						timeout := time.NewTicker(time.Duration(50) * time.Millisecond)
						select {
						case b.Callbacks[d.CorrelationId] <- d:

						case <-timeout.C:
							log.Printf("Timeoutas suveike: %s", d.CorrelationId)
						}
						timeout.Stop()
					}()
				}
			case <-b.ctx.Done():
				log.Println("Listening service has stopped.")
				b.wg.Done()

				return
			}
		}
	}()
}

// Call function puts randomly generates corr id and a channel into a map
// Publishes message to Rabbit MQ and after receiving a response, unmarshals the message and returns response
// If Timeout - return nil
func (b *Boss) Call(worker string, task string, payload interface{}, ret interface{}) error {

	timeout := time.NewTicker(time.Duration(50) * time.Millisecond)
	corrID := generateCorrID(32)
	b.Callbacks[corrID] = make(chan amqp.Delivery, 1)

	defer func() {
		close(b.Callbacks[corrID])
		delete(b.Callbacks, corrID)
		timeout.Stop()
	}()

	gob, err := GobMarshal(payload)
	if err != nil {
		log.Println("Error while Marshaling data: ", err)

		return err
	}

	b.Ch.Publish(
		b.Exchange, // exchange
		worker,     // routing key
		true,       // mandatory
		false,      // immediate
		amqp.Publishing{
			Headers: amqp.Table{
				"action": task,
			},
			ContentType:   "application/gob",
			CorrelationId: corrID,
			ReplyTo:       b.QueueName,
			Body:          gob,
		},
	)

	select {
	case packet := <-b.Callbacks[corrID]:
		GobUnmarshal(packet.Body, ret)

	case <-timeout.C:
		err = fmt.Errorf("Timeouted")
	}
	return err
}
