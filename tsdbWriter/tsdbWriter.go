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
	"github.com/jaredmcqueen/quote-writer/util"
	"github.com/prometheus/client_golang/prometheus"
)

func TimeScaleTableCreator(config util.Config) error {

	tableSQL := `
        CREATE TABLE IF NOT EXISTS quotes (
          time TIMESTAMPTZ NOT NULL, 
          symbol VARCHAR, 
          ask_exchange VARCHAR, 
          ask_price DOUBLE PRECISION, 
          ask_size int NOT NULL, 
          bid_exchange VARCHAR, 
          bid_price DOUBLE PRECISION, 
          bid_size int NOT NULL, 
          conditions VARCHAR ARRAY, 
          tape VARCHAR
        );
        SELECT create_hypertable('quotes', 'time', chunk_time_interval => 86400000, if_not_exists => TRUE);
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

func TimescaleWriter(id int, streamChan <-chan map[string]interface{}, config util.Config, counter *prometheus.CounterVec) {

	columns := []string{
		"time",
		"symbol",
		"ask_exchange",
		"ask_price",
		"ask_size",
		"bid_exchange",
		"bid_price",
		"bid_size",
		"conditions",
		"tape",
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
	var askPrice float64
	var askSize int64
	var bidPrice float64
	var bidSize int64
	var conditions []string
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
		counter.WithLabelValues(fmt.Sprintf("%v", id)).Add(float64(len(batch)))
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
			askPrice, _ = strconv.ParseFloat(t["ap"].(string), 64)
			askSize, _ = strconv.ParseInt(t["as"].(string), 10, 64)
			bidPrice, _ = strconv.ParseFloat(t["bp"].(string), 64)
			bidSize, _ = strconv.ParseInt(t["bs"].(string), 10, 64)
			conditions = strings.Split(t["c"].(string), "")

			quote = []interface{}{
				time.UnixMilli(timeMilli),
				t["S"],
				t["ax"],
				askPrice,
				askSize,
				t["bx"],
				bidPrice,
				bidSize,
				conditions,
				t["z"],
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
