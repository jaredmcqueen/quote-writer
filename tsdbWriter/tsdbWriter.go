package tsdbWriter

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/jaredmcqueen/tsdb-writer/streamReader"
	"github.com/jaredmcqueen/tsdb-writer/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	streamChan = streamReader.StreamChan
	TSDBChan   = make(chan BatchItem)
	barSql     = `
        INSERT INTO "bars" (time, symbol, high, low, volume) values
        ($1, $2, $3, $4, $5);
    `
	quoteSQL = `
        INSERT INTO "quotes" (time, symbol, high, low) values
        ($1, $2, $3, $4);
    `
	statusSQL = `
        INSERT INTO "statuses" (
        time, symbol, status_code, status_message, reason_code, reason_message, tape) values
        ($1, $2, $3, $4, $5, $6, $7);
    `
	tradeSQL = `
        INSERT INTO "trades" (
        time, symbol, price, tradeSize, tradeCondition, exchangeCode, tape) values
        ($1, $2, $3, $4, $5, $6, $7);
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

func ProcessStreams() {
	for streamMessage := range streamChan {
		// fmt.Println("read from streamChan", streamMessage)
		switch messageType := streamMessage["T"].(string); messageType {
		case "bars":
			dateMilli, _ := strconv.ParseInt(streamMessage["t"].(string), 10, 64)
			unixTime := time.UnixMilli(dateMilli).Format(time.RFC3339Nano)
			TSDBChan <- BatchItem{
				query: barSql,
				arguments: []interface{}{
					unixTime,
					streamMessage["S"],
					streamMessage["h"],
					streamMessage["l"],
					streamMessage["v"],
				},
			}
			tsdbCounter.WithLabelValues(messageType).Inc()
		case "quotes":
			dateMilli, _ := strconv.ParseInt(streamMessage["t"].(string), 10, 64)
			unixTime := time.UnixMilli(dateMilli).Format(time.RFC3339Nano)
			TSDBChan <- BatchItem{
				query: quoteSQL,
				arguments: []interface{}{
					unixTime,
					streamMessage["S"],
					streamMessage["h"],
					streamMessage["l"],
				},
			}
			tsdbCounter.WithLabelValues(messageType).Inc()
		case "statuses":
			dateMilli, _ := strconv.ParseInt(streamMessage["t"].(string), 10, 64)
			unixTime := time.UnixMilli(dateMilli).Format(time.RFC3339Nano)
			TSDBChan <- BatchItem{
				query: statusSQL,
				arguments: []interface{}{
					unixTime,
					streamMessage["S"],
					streamMessage["sc"],
					streamMessage["sm"],
					streamMessage["rc"],
					streamMessage["rm"],
					streamMessage["z"],
				},
			}
			tsdbCounter.WithLabelValues(messageType).Inc()
		case "trades":
			dateMilli, _ := strconv.ParseInt(streamMessage["t"].(string), 10, 64)
			unixTime := time.UnixMilli(dateMilli).Format(time.RFC3339Nano)
			TSDBChan <- BatchItem{
				query: tradeSQL,
				arguments: []interface{}{
					unixTime,
					streamMessage["S"],
					streamMessage["p"],
					streamMessage["s"],
					strings.Split(streamMessage["c"].(string), ""),
					streamMessage["x"],
					streamMessage["z"],
				},
			}
			tsdbCounter.WithLabelValues(messageType).Inc()
		default:
			fmt.Println(streamMessage)
			log.Fatal("found weird message")
		}
	}
}

func DBWriter(worker string) {
	ctx := context.Background()
	dbpool, err := pgxpool.Connect(ctx, util.Config.TSDBConnection)
	if err != nil {
		log.Fatal("cannot connect to TSDB", err)
	}
	log.Println("connected to TSDB")
	timeout := time.Duration(time.Duration(util.Config.TSDBatchTimeout) * time.Millisecond)
	timer := time.NewTimer(timeout)

	batch := &pgx.Batch{}
	debugBatch := []interface{}{}

	sendData := func() {
		br := dbpool.SendBatch(ctx, batch)
		_, err := br.Exec()
		if err != nil {
			fmt.Println(debugBatch...)
			log.Fatal("error sending batch to db", err)
		}
		batch = &pgx.Batch{}
		debugBatch = debugBatch[:0]
		br.Close()
	}

	for {
		select {
		case <-timer.C:
			if batch.Len() > 0 {
				sendData()
			}
			timer.Reset(timeout)
		case bi := <-TSDBChan:
			batch.Queue(bi.query, bi.arguments...)
			debugBatch = append(debugBatch, bi)

			if batch.Len() >= util.Config.TSDBBatchSize {
				sendData()
				if !timer.Stop() {
					<-timer.C
				}
				timer.Reset(timeout)
			}
		}
	}
}
