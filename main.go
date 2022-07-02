package main

import (
	"fmt"
	"log"
	"net/http"

	"os"
	"os/signal"

	"github.com/jaredmcqueen/tsdb-writer/streamReader"
	"github.com/jaredmcqueen/tsdb-writer/tsdbWriter"
	"github.com/jaredmcqueen/tsdb-writer/util"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	// table creation needs a single goroutine
	tsdbWriter.TSDBTableCreator()

	// TSDB writer
	for i := 1; i < util.Config.TSDBWorkers+1; i++ {
		go tsdbWriter.DBWriter(fmt.Sprintf("worker-%d", i))
	}

	// consumes redis streams
	go streamReader.RedisConsumer()

	// processes each type of stream
	go tsdbWriter.ProcessStreams()

	// metrics
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(":9100", nil)
	}()

	<-signalChan
	log.Println("exiting app")
}
