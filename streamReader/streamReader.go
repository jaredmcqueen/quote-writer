package streamReader

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/go-redis/redis/v9"
	"github.com/jaredmcqueen/tsdb-writer/util"
)

var (
	StreamChan = make(chan map[string]interface{})
)

// RedisConsumer reads from multiple streams, keeping point-in-times for each
// all message.Values are sent to streamChan without modification
func RedisConsumer() {
	ctx := context.Background()
	log.Println("connecting to redis endpoint", util.Config.RedisEndpoint)
	rdb := redis.NewClient(&redis.Options{
		Addr: util.Config.RedisEndpoint,
	})

	// test redis connection
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.Fatal("could not connect to redis", err)
	}
	log.Println("connected to redis")

	pitMap := make(map[string]string)
	for _, streamName := range strings.Split(util.Config.RedisStreamNames, " ") {
		pitMap[streamName] = util.Config.RedisStreamStart
	}

	newpits := func() []string {
		streams := []string{}
		pits := []string{}
		for s, p := range pitMap {
			streams = append(streams, s)
			pits = append(pits, p)
		}
		return append(streams, pits...)
	}

	for {
		fmt.Println(pitMap)
		items, err := rdb.XRead(ctx,
			&redis.XReadArgs{
				Streams: newpits(),
				Count:   util.Config.RedisStreamCount,
				Block:   time.Duration(time.Second),
			},
		).Result()
		if err != nil {
			log.Println("error XRead: ", err)
		}
		for _, stream := range items {
			for _, message := range stream.Messages {
				StreamChan <- message.Values
				pitMap[stream.Stream] = message.ID
			}
		}
	}
}
