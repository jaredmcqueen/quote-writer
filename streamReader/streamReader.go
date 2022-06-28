package streamReader

import (
	"context"
	"log"
	"time"

	"github.com/go-redis/redis/v9"
	"github.com/jaredmcqueen/quote-writer/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	config     = util.Config
	StreamChan = make(chan map[string]interface{})

	redisCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "quote_writer_redis_reads_total",
		Help: "total amount of events pulled from redis",
	})
)

func RedisConsumer() {
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
					StreamChan <- message.Values
					pit = message.ID
					redisCounter.Inc()
				}
			}
		}
	}
}
