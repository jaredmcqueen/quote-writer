package streamReader

import (
	"context"
	"log"
	"time"

	"github.com/go-redis/redis/v9"
	"github.com/jaredmcqueen/quote-writer/util"
	"github.com/prometheus/client_golang/prometheus"
)

func RedisConsumer(streamChan chan<- map[string]interface{}, config util.Config, counter prometheus.Counter) {
	ctx := context.Background()
	log.Println("connecting to redis endpoint", config.RedisEndpoint)
	rdb := redis.NewClient(&redis.Options{
		Addr: config.RedisEndpoint,
	})

	// test redis connection
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.Fatal("could not connect to redis", err)
	}
	log.Println("connected to redis")

	// create a point in time to start reading the stream
	pit := config.RedisStreamStart
	for {
		items, err := rdb.XRead(ctx,
			&redis.XReadArgs{
				Streams: []string{"quotes", pit},
				Count:   config.RedisStreamCount,
				Block:   0,
			},
		).Result()
		if err != nil {
			log.Println("error XRead: ", err)
			log.Println("attempting again in 10 seconds")
			time.Sleep(time.Second * 10)
			continue
		}

		for _, stream := range items {
			if stream.Stream == "quotes" {
				for _, message := range stream.Messages {
					streamChan <- message.Values
					pit = message.ID
					counter.Inc()
				}
			}
		}
	}
}
