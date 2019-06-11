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

	Boss := boss.NewBoss(ctx, "amqp://guest:guest@localhost:5672", 3, "testExchange", "testQueue")

	Boss.Start()

	<-stop

	Stop(wg, cancel, *Boss)
}

func Stop(wg *sync.WaitGroup, cancel context.CancelFunc, Boss boss.Boss) {

	cancel()

	Boss.Stop()
}
