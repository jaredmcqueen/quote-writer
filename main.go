package main

import (
	"log"
	"net/http"

	"os"
	"os/signal"

	"github.com/jaredmcqueen/tsdb-writer/reader"
	"github.com/jaredmcqueen/tsdb-writer/util"
	"github.com/jaredmcqueen/tsdb-writer/writers"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	// create necessary postgreSQL tables
	if util.Config.PostgreSQLEnabled {
		err := util.TSDBTableCreator()
		if err != nil {
			log.Fatal("error creating postgreSQL tables", err)
		}
		log.Println("created postgreSQL tables")
	}

	ps := util.NewPubsub()

	// start up connections to writers
	if util.Config.PostgreSQLEnabled {
		go writers.PostgreSQLWriter(ps)
	}

	if util.Config.RedisTSEnabled {
		go writers.RedisTSWriter(ps)
	}

	go reader.RedisStreamsReader(ps)

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(":9100", nil)
	}()

	log.Println("ready")
	<-signalChan
	log.Println("exiting app")
}
