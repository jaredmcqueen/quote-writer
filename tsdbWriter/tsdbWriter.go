package tsdbWriter

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/jaredmcqueen/quote-writer/streamReader"
	"github.com/jaredmcqueen/quote-writer/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	config      = util.Config
	streamChan  = streamReader.StreamChan
	TSDBCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "quote_writer_tsdb_writes_total",
		Help: "how many inserts get written to TimescaleDB",
	}, []string{"worker"})
)

func TimeScaleTableCreator() error {
	tableSQL := `
        CREATE TABLE IF NOT EXISTS quotes (
          time TIMESTAMPTZ NOT NULL, 
          symbol VARCHAR, 
          high DOUBLE PRECISION, 
          low DOUBLE PRECISION
        );
        SELECT create_hypertable(
            'quotes', 'time', chunk_time_interval => 86400000, if_not_exists => TRUE
        );
        `

	ctx := context.Background()
	dbpool, err := pgxpool.Connect(ctx, config.TimescaleDBConnection)
	if err != nil {
		log.Fatal("cannot connect to TSDB", err)
	}
	log.Println("connected to TSDB")

	_, err = dbpool.Exec(ctx, tableSQL)
	if err != nil {
		return err
	}
	return nil
}

func TimescaleWriter(id int) {
	columns := []string{
		"time",
		"symbol",
		"high",
		"low",
	}

	ctx := context.Background()
	dbpool, err := pgxpool.Connect(ctx, config.TimescaleDBConnection)
	if err != nil {
		log.Fatal("cannot connect to TSDB", err)
	}
	log.Println("connected to TSDB")

	timeout := time.Duration(time.Duration(config.TimescaleDBBatchTimeout) * time.Millisecond)
	timer := time.NewTimer(timeout)

	var timeMilli int64
	var high float64
	var low float64
	var quote []interface{}
	var batch [][]interface{}

	sendData := func() {
		_, err := dbpool.CopyFrom(
			ctx,
			pgx.Identifier{"quotes"},
			columns,
			pgx.CopyFromRows(batch),
		)
		if err != nil {
			fmt.Println("error sending data ", err)
		}
		TSDBCounter.WithLabelValues(fmt.Sprintf("%v", id)).Add(float64(len(batch)))
		batch = batch[:0]
	}

	for {
		select {
		case <-timer.C:
			if len(batch) > 0 {
				sendData()
			}
			timer.Reset(timeout)
		case t := <-streamChan:
			timeMilli, _ = strconv.ParseInt(t["t"].(string), 10, 64)
			high, _ = strconv.ParseFloat(t["h"].(string), 64)
			low, _ = strconv.ParseFloat(t["l"].(string), 64)

			quote = []interface{}{
				time.UnixMilli(timeMilli),
				t["S"],
				high,
				low,
			}

			batch = append(batch, quote)

			if len(batch) >= config.TimescaleDBBatchSize {
				sendData()
				if !timer.Stop() {
					<-timer.C
				}
				timer.Reset(timeout)
			}
		}
	}
}
