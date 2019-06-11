package main

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/AntanasMaziliauskas/rabbitas/boss"
)

func main() {
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM, syscall.SIGSTOP, syscall.SIGKILL)

	Worker := boss.NewWorker(ctx, "amqp://guest:guest@localhost:5672", 3, "testExchange", "testQueueWorker")

	Worker.StartWork()

	<-stop

	Stop(wg, cancel, *Worker)
}

func Stop(wg *sync.WaitGroup, cancel context.CancelFunc, Worker boss.Boss) {

	cancel()

	Worker.StopWork()
}
