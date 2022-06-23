package main

import (
	"log"
	"net/http"
	"os"
	"os/signal"

	"github.com/jaredmcqueen/quote-writer/streamReader"
	"github.com/jaredmcqueen/quote-writer/tsdbWriter"
	"github.com/jaredmcqueen/quote-writer/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	config           util.Config
	promRedisCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "quote_writer_redis_reads_total",
		Help: "total amount of events pulled from redis",
	})
	promTSDBCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "quote_writer_tsdb_writes_total",
		Help: "how many inserts get written to TimescaleDB",
	}, []string{"worker"})
)

func main() {
	// load config
	config, err := util.LoadConfig(".")
	if err != nil {
		log.Fatal("could not load config", err)
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	streamChan := make(chan map[string]interface{})

	// run redis stream reader

	err = tsdbWriter.TimeScaleTableCreator(config)
	if err != nil {
		log.Fatal("error creating table ", err)
	}

	for i := 1; i < config.TimescaleDBWorkers+1; i++ {
		go tsdbWriter.TimescaleWriter(i, streamChan, config, promTSDBCounter)
	}

	go streamReader.RedisConsumer(streamChan, config, promRedisCounter)

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(":9100", nil)
	}()

	<-signalChan
	log.Println("exiting app")
}
