package dispatcher

import (
	"log"
	"github.com/streadway/amqp"
)


//atskiras failas sitam visam
type TaskResponse struct {
	corrID  string
	replyTo string
	Body    []byte
	app     *Worker
}



// Publish function sends a message to RabbitMQ
func (n *TaskResponse) Publish(payload []byte) {
	err := n.app.ch.Publish(
		n.app.exchange, // exchange
		n.replyTo,      // routing key
		false,          // mandatory
		false,          // immediate
		amqp.Publishing{
			Headers: amqp.Table{
				"name": n.app.queueName,
			},
			ContentType:   "application/gob",
			CorrelationId: n.corrID,
			Body:          payload,
		})
	if err != nil {
		log.Println("Failed to send message to RabbitMQ. Error: ", err)
	}
}