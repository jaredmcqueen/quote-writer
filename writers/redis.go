package writers

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/go-redis/redis/v9"
	"github.com/jaredmcqueen/tsdb-writer/util"
)

func RedisTSWriter(pubsub *util.Pubsub) {
	fmt.Println("started RedisTSWriter")
	ctx := context.Background()
	log.Println("connecting to redis endpoint", util.Config.RedisTSEndpoint)
	rdb := redis.NewClient(&redis.Options{
		Addr: util.Config.RedisTSEndpoint,
	})

	// test redis connection
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.Fatal("could not connect to redis", err)
	}
	log.Println("connected to redis")

	redisRead := pubsub.Subscribe("redis")
	redisWrite := make(chan []interface{}, util.Config.BatchSize)

	// the actual writer
	go func() {
		timeout := time.Duration(time.Duration(util.Config.BatchTimeout) * time.Millisecond)
		timer := time.NewTimer(timeout)
		batch := rdb.Pipeline()

		sendData := func() {
			_, err := batch.Exec(ctx)
			if err != nil {
				log.Println("error execing pipeline", err)
			}
		}

		for {
			select {
			case <-timer.C:
				if batch.Len() > 0 {
					sendData()
				}
				timer.Reset(timeout)

			case m := <-redisWrite:
				batch.Do(ctx, m...)

				if batch.Len() >= util.Config.BatchSize {
					sendData()
					if !timer.Stop() {
						<-timer.C
					}
					timer.Reset(timeout)
				}
			}
		}
	}()

	// to process the data
	go func() {
		for m := range redisRead {
			message := m.(map[string]interface{})
			switch messageType := message["T"].(string); messageType {
			case "quotes":
				for _, field := range []string{"h", "l"} {
					redisWrite <- []interface{}{
						"TS.ADD",
						fmt.Sprintf("quotes:%v:%v", message["S"], field),
						message["t"],
						fmt.Sprintf("%v", message[field]),
						"LABELS",
						"type", "quotes",
						"symbol", message["S"],
					}
				}
			case "trades":
				for _, field := range []string{"v"} {
					redisWrite <- []interface{}{
						"TS.ADD",
						fmt.Sprintf("trades:%v:%v", message["S"], field),
						message["t"],
						fmt.Sprintf("%v", message[field]),
						"LABELS",
						"type", "trades",
						"symbol", message["S"],
					}
				}
			case "statuses":
				continue
			case "bars":
				continue
			default:
				fmt.Println(message)
				log.Fatal("found weird message in Redis Stream")
			}
		}
	}()
}
