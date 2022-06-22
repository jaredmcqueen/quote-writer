package main

import (
	"log"
	"os"
	"os/signal"

	"github.com/jaredmcqueen/quote-writer/streamReader"
	"github.com/jaredmcqueen/quote-writer/tsdbWriter"
	"github.com/jaredmcqueen/quote-writer/util"
)

var config util.Config

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

	for i := 0; i < config.TimescaleDBWorkers; i++ {
		go tsdbWriter.TimescaleWriter(streamChan, config)
	}

	go streamReader.RedisConsumer(streamChan, config)

	<-signalChan
	log.Println("exiting app")
}
