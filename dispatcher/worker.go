package dispatcher

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

type Worker struct {
	address    string
	conn       *amqp.Connection
	closeError chan *amqp.Error
	timeout    int
	ch         *amqp.Channel
	exchange   string
	queueName  string
	consume    <-chan amqp.Delivery
	tasks      map[string]func(TaskResponse)

	wg     *sync.WaitGroup
	cancel context.CancelFunc
	ctx    context.Context
}


// NewWorker takes all neccesary variables and places them into Boss structure
func NewWorker(address string) *Worker {

	return &Worker{
		address:   address,
		timeout:   3,
		exchange:  "DispExchange",
		queueName: "Worker",
	}

}

// Listen is used to make connection with RabbitMQ and
// start go routine which listes for incomming messages
func (w *Worker) Listen(ctx context.Context) {
	w.ctx = ctx
	w.tasks = make(map[string]func(TaskResponse))
	w.closeError = make(chan *amqp.Error)
	w.wg = &sync.WaitGroup{}

	w.connector()

	w.listener()

	w.stop()

}


func (w *Worker) SetTimeout(timeout int) {
	w.timeout = timeout
	}
	
	func (w *Worker) SetExchangeName(exchange string) {
	w.exchange = exchange
	}
	
	func (w *Worker) SetQueueName(queue string) {
	w.queueName = queue
	}
	
	func (w *Worker) SetBind(bind string) error {
		
		return w.ch.QueueBind(
			w.queueName, // queue name
			w.queueName, // routing key
			w.exchange,  // exchange
			false,
			nil)
	}


// stop function checks to see if connection and channel is closed before closing them
func (w *Worker) stop() {
	w.wg.Add(1)
	go func() {
		for {
			select {
			case <-w.ctx.Done():
				if w.conn != nil {
					w.conn.Close()
				}
				if w.ch != nil {
					w.ch.Close()
				}
				w.wg.Done()

				return
			}
		}
	}()
}

// listen function starts listening service that listens for incomming
// RabbitMQ messages
func (w *Worker) listener() {
	log.Println("Started Listening service")
	w.wg.Add(1)
	go func() {
		for {
			select {
			case d, ok := <-w.consume:
				if !ok {
					log.Println("Rabbit channel has been closed")
					w.wg.Done()
					return
				}

				if d.Headers != nil {
					log.Printf("Received a message: %s", d.Headers["action"])
					resp, _ := w.prepareResponse(d)
					w.tasks[d.Headers["action"].(string)](*resp)
				}
			case <-w.ctx.Done():
				log.Println("Listening service has stopped.")
				w.wg.Done()

				return
			}
		}
	}()
}

func (w *Worker) prepareResponse(cmd amqp.Delivery) (*TaskResponse, error) {
	return &TaskResponse{
		corrID:  cmd.CorrelationId,
		replyTo: cmd.ReplyTo,
		Body:    cmd.Body,
		app:     w,
	}, nil
}


// Register is used to register new task
func (w *Worker) Register(task string, callback func(TaskResponse)) {
	w.tasks[task] = callback
}

func (w *Worker) connector() {
	var (
		rabbitErr *amqp.Error
		err       error
	)

	w.conn, err = amqp.Dial(w.address)
	if err != nil {
		log.Fatal("Cant connect to RabbitMQ. Error: ", err)
	}
	w.conn.NotifyClose(w.closeError)
	w.ch, w.consume = setRabbit(w.conn, w.exchange, w.queueName, "broadcast")
	log.Println("Connected to RabbitMQ")
	retry := &time.Ticker{}
	w.wg.Add(1)
	go func() {
		for {
			select {
			case <-w.ctx.Done():
				log.Println("Rabbit connector has stopped.")
				w.wg.Done()

				return

			case rabbitErr = <-w.closeError:
				if rabbitErr != nil {
					retry = time.NewTicker(time.Duration(w.timeout) * time.Second)
				}

			case <-retry.C:
				log.Println("Trying to reconnect to RabbitMQ...")
				w.conn, err = amqp.Dial(w.address)
				if err == nil {
					log.Println("Successfully reconnected to RabbitMQ")
					w.closeError = make(chan *amqp.Error)
					w.conn.NotifyClose(w.closeError)

					w.ch, w.consume = setRabbit(w.conn, w.exchange, w.queueName, "broadcast")
					w.listener()
					retry.Stop()

					continue
				}
				log.Println("Unable to connect..")
			}
		}
	}()
}
