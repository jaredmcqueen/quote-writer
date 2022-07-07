package writers

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/jaredmcqueen/tsdb-writer/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	barSql = `
        INSERT INTO "bars" (time, symbol, high, low, volume) values
        ($1, $2, $3, $4, $5);
    `
	statusSQL = `
        INSERT INTO "statuses" (
        time, symbol, status_code, status_message, reason_code, reason_message, tape) values
        ($1, $2, $3, $4, $5, $6, $7);
    `
	quoteSQL = `
        INSERT INTO "quotes" (
        time, symbol, high, low) values
        ($1, $2, $3, $4);
    `

	tradeSQL = `
        INSERT INTO "trades" (
        time, symbol, high, low, volume) values
        ($1, $2, $3, $4, $5);
    `
	tsdbCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "tsdb_writer_sql_batch_total",
		Help: "how many inserts get written to TimescaleDB",
	}, []string{"type"})
)

type BatchItem struct {
	query     string
	arguments []interface{}
}

func PostgreSQLWriter(pubsub *util.Pubsub) {
	fmt.Println("started PostgreSQLWriter")
	ctx := context.Background()
	dbpool, err := pgxpool.Connect(ctx, util.Config.PostgreSQLEndpoint)
	if err != nil {
		log.Fatal("cannot connect to TSDB", err)
	}
	log.Println("connected to postgreSQL")

	// from the pubsub
	redisMessages := pubsub.Subscribe("redis")

	// local channel
	sqlMessages := make(chan BatchItem, util.Config.CacheSize)

	// the actual writer
	go func() {
		timeout := time.Duration(time.Duration(util.Config.BatchTimeout) * time.Millisecond)
		timer := time.NewTimer(timeout)
		batch := &pgx.Batch{}

		sendData := func() {
			br := dbpool.SendBatch(ctx, batch)
			_, err := br.Exec()
			if err != nil {
				log.Fatal("error sending batch to db", err)
			}
			batch = &pgx.Batch{}
			br.Close()
		}

		for {
			select {
			case <-timer.C:
				if batch.Len() > 0 {
					sendData()
				}
				timer.Reset(timeout)
			case m := <-sqlMessages:
				batch.Queue(m.query, m.arguments...)

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

		unixTime := func(milli interface{}) string {
			dateMilli, _ := strconv.ParseInt(milli.(string), 10, 64)
			return time.UnixMilli(dateMilli).Format(time.RFC3339Nano)

		}

		for m := range redisMessages {
			message := m.(map[string]interface{})
			// fmt.Println("read from streamChan", streamMessage)
			switch messageType := message["T"].(string); messageType {
			case "bars":
				sqlMessages <- BatchItem{
					query: barSql,
					arguments: []interface{}{
						unixTime(message["t"]),
						message["S"],
						message["h"],
						message["l"],
						message["v"],
					},
				}
				tsdbCounter.WithLabelValues(messageType).Inc()
			case "statuses":
				sqlMessages <- BatchItem{
					query: statusSQL,
					arguments: []interface{}{
						unixTime(message["t"]),
						message["S"],
						message["sc"],
						message["sm"],
						message["rc"],
						message["rm"],
						message["z"],
					},
				}
				tsdbCounter.WithLabelValues(messageType).Inc()
			case "trades":
				sqlMessages <- BatchItem{
					query: tradeSQL,
					arguments: []interface{}{
						unixTime(message["t"]),
						message["S"],
						message["h"],
						message["l"],
						message["v"],
					},
				}
				tsdbCounter.WithLabelValues(messageType).Inc()
			case "quotes":
				sqlMessages <- BatchItem{
					query: quoteSQL,
					arguments: []interface{}{
						unixTime(message["t"]),
						message["S"],
						message["h"],
						message["l"],
					},
				}
				tsdbCounter.WithLabelValues(messageType).Inc()
			default:
				fmt.Println(message)
				log.Fatal("found weird message")
			}
		}
	}()
}
