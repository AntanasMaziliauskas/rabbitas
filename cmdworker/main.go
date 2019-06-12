package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/AntanasMaziliauskas/rabbitas/dispatcher"
)

func main() {
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM, syscall.SIGSTOP, syscall.SIGKILL)

	Worker := dispatcher.NewWorker("amqp://guest:guest@localhost:5672")
	Worker.Listen(ctx)
	Worker.Register("Add", Add)

	<-stop

	cancel()
	wg.Wait()
}

func Add(resp dispatcher.TaskResponse) {
	message := "string"
	dispatcher.GobUnmarshal(resp.Body, &message)
	log.Println(message)

	//Kaip cia galiu padaryt, kad nereiketu paduoti exchange name?
	resp.Publish(nil)
}
