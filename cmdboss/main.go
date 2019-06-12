package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/AntanasMaziliauskas/rabbitas/dispatcher"
)

func main() {
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM, syscall.SIGSTOP, syscall.SIGKILL)

	Boss := dispatcher.NewBoss("amqp://guest:guest@localhost:5672")

	Boss.Listen(ctx)

	time.Sleep(time.Duration(50) * time.Millisecond)
	err := Boss.Call("Worker", "Add", "hello", nil)
	if err != nil {
		log.Println(err)
	}
	<-stop

	cancel()
	wg.Wait()
}
