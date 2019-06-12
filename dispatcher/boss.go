package dispatcher

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

// Boss structure is the main structure used with Boss methods
type Boss struct {
	address    string
	conn       *amqp.Connection
	closeError chan *amqp.Error
	timeoutConn    int
	timeoutCall int
	timeoutBroadcast int
	ch         *amqp.Channel
	exchange   string
	queueName  string
	consume    <-chan amqp.Delivery
	callbacks  map[string]chan amqp.Delivery

	wg     *sync.WaitGroup
	cancel context.CancelFunc
	ctx    context.Context
}

// NewBoss takes all neccesary variables and places them into Boss structure
func NewBoss(address string) *Boss {

	return &Boss{
		address:   address,
		timeoutConn:   3,
		timeoutCall: 50,
		timeoutBroadcast: 50,
		exchange:  "DispExchange",
		queueName: "Boss",
	}

}

// Listen is used to make connection with RabbitMQ and
// start go routine which listes for replys
func (b *Boss) Listen(ctx context.Context) {
	b.ctx = ctx
	b.callbacks = make(map[string]chan amqp.Delivery, 1)
	b.closeError = make(chan *amqp.Error)
	b.wg = &sync.WaitGroup{}

	b.connector()

	b.listener()

	b.stop()
}


// Call function puts randomly generated corr id and a channel into a map
// Publishes message to Rabbit MQ and after receiving a response, unmarshals the message and returns response
// If Timeout - return nil
func (b *Boss) Call(worker string, task string, payload interface{}, ret interface{}) error {

	timeout := time.NewTicker(time.Duration(b.timeoutCall) * time.Millisecond)
	corrID := generateCorrID(32)
	b.callbacks[corrID] = make(chan amqp.Delivery, 1)

	defer func() {
		close(b.callbacks[corrID])
		delete(b.callbacks, corrID)
		timeout.Stop()
	}()

	gob, err := GobMarshal(payload)
	if err != nil {
		log.Println("Error while Marshaling data: ", err)

		return err
	}

	b.ch.Publish(
		b.exchange, // exchange
		worker,     // routing key
		true,       // mandatory
		false,      // immediate
		amqp.Publishing{
			Headers: amqp.Table{
				"action": task,
			},
			ContentType:   "application/gob",
			CorrelationId: corrID,
			ReplyTo:       b.queueName,
			Body:          gob,
		},
	)

	select {
	case packet := <-b.callbacks[corrID]:
		GobUnmarshal(packet.Body, ret)

	case <-timeout.C:
		err = fmt.Errorf("Timeouted")
	}
	return err
}

// Broadcast function makes a map of randomly generates CorrID and a channel
// Publishes a message to a workers. Ignores empty responses. 
// On expected response return data. On timeout returns error.
func (b *Boss) Broadcast(command string, payload interface{}, ret interface{}) error {
	timeout := time.NewTicker(time.Duration(b.timeoutBroadcast) * time.Millisecond)
	corrID := generateCorrID(32)
	b.callbacks[corrID] = make(chan amqp.Delivery, 1)

	defer func() {
		close(b.callbacks[corrID])
		delete(b.callbacks, corrID)
	}()

	gob, _ := GobMarshal(payload)
	err := b.ch.Publish(
		b.exchange, 	  // exchange
		"broadcast",     // routing key
		true,             // mandatory
		false,            // immediate
		amqp.Publishing{
			Headers:       map[string]interface{}{"action": command},
			ContentType:   "aplication/gob",
			CorrelationId: corrID,
			ReplyTo:       b.queueName,
			Body:          gob,
		},
	)
	if err != nil {
		log.Println("Error while publishing ", err)
	}

	for  {
		select {
		case packet := <-b.callbacks[corrID]:
			if packet.Body != nil {
				GobUnmarshal(packet.Body, &ret)

				timeout.Stop()

				return nil
			}
			continue

		case <-timeout.C:
			timeout.Stop()

			return fmt.Errorf("Timeouted")
		}
	}
}

// SetConnTimeout is used to set a timeout for reconnection.
// Time is in seconds and default value is 3 seconds
func (b *Boss) SetConnTimeout(timeout int) {
b.timeoutConn = timeout
}

// SetCallTimeout is used to set a timeout for Call function.
// It is the time to wait for response from worker.
// Time is in miliseconds and default value is 50 miliseconds
func (b *Boss) SetCallTimeout(timeout int) {
	b.timeoutCall = timeout
}

// SetBroadcastTimeout is used to set a timeout for Call function.
// It is the time to wait for response from worker.
// Time is in miliseconds and default value is 50 miliseconds
func (b *Boss) SetBroadcastTimeout(timeout int) {
	b.timeoutBroadcast = timeout
}

// SetExchangeName is used to set exchange name.
// Default exchange is "DispExchange"
func (b *Boss) SetExchangeName(exchange string) {
b.exchange = exchange
}

// SetQueueName is used to set queue name.
// Default queue name is "Boss"
func (b *Boss) SetQueueName(queue string) {
b.queueName = queue
}

// SetRoutingKey is used to set bind for the queue
// Default bind is Queue name
func (b *Boss) SetRoutingKey(routingKey string) error {
	
	return b.ch.QueueBind(
		b.queueName, // queue name
		routingKey, // routing key
		b.exchange,  // exchange
		false,
		nil)
}

// Stop function checks to see if connection and channel is closed before closing them
func (b *Boss) stop() {
	b.wg.Add(1)
	go func() {
		for {
			select {
			case <-b.ctx.Done():
				if b.conn != nil {
					b.conn.Close()
				}
				if b.ch != nil {
					b.ch.Close()
				}
				b.wg.Done()

				return
			}
		}
	}()
}

func (b *Boss) listener() {
	b.wg.Add(1)
	go func() {
		log.Println("Listening to Rabbit for incomming messages")
		for {
			select {
			case d, ok := <-b.consume:
				if !ok {
					log.Println("RabbitMQ channel has been closed")
					b.wg.Done()
					return
				}
				log.Printf("Server received reply. %#v", d.Headers)
				if _, exists := b.callbacks[d.CorrelationId]; exists {
					go func() {
						timeout := time.NewTicker(time.Duration(b.timeoutCall) * time.Millisecond)
						select {
						case b.callbacks[d.CorrelationId] <- d:

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


func (b *Boss) connector() {
	var (
		rabbitErr *amqp.Error
		err       error
	)

	b.conn, err = amqp.Dial(b.address)
	if err != nil {
		log.Fatal("Cant connect to RabbitMQ. Error: ", err)
	}
	b.conn.NotifyClose(b.closeError)
	b.ch, b.consume = setRabbit(b.conn, b.exchange, b.queueName, "")
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

			case rabbitErr = <-b.closeError:
				if rabbitErr != nil {
					retry = time.NewTicker(time.Duration(b.timeoutConn) * time.Second)
				}

			case <-retry.C:
				log.Println("Trying to reconnect to RabbitMQ...")
				b.conn, err = amqp.Dial(b.address)
				if err == nil {
					log.Println("Successfully reconnected to RabbitMQ")
					b.closeError = make(chan *amqp.Error)
					b.conn.NotifyClose(b.closeError)

					b.ch, b.consume = setRabbit(b.conn, b.exchange, b.queueName, "")
					b.listener()
					retry.Stop()

					continue
				}
				log.Println("Unable to connect..")
			}
		}
	}()
}
