package main

import (
	"log"
	"net/http"
	"os"
	"os/signal"

	"github.com/jaredmcqueen/quote-writer/streamReader"
	"github.com/jaredmcqueen/quote-writer/tsdbWriter"
	"github.com/jaredmcqueen/quote-writer/util"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	// table creation needs a single goroutine
	err := tsdbWriter.TimeScaleTableCreator()
	if err != nil {
		log.Fatal("error creating table ", err)
	}

	// TSDB writer
	for i := 1; i < util.Config.TimescaleDBWorkers+1; i++ {
		go tsdbWriter.TimescaleWriter(i)
	}

	// redis reader
	go streamReader.RedisConsumer()

	// metrics
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(":9100", nil)
	}()

	<-signalChan
	log.Println("exiting app")
}
