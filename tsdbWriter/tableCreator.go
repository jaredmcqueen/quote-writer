package tsdbWriter

import (
	"context"
	"fmt"
	"log"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/jaredmcqueen/tsdb-writer/util"
)

func TSDBTableCreator() {

	barSQL := `
    CREATE TABLE IF NOT EXISTS bars ( 
        time TIMESTAMPTZ,
        symbol VARCHAR,
        high DOUBLE PRECISION,
        low DOUBLE PRECISION,
        volume int
    );
    SELECT create_hypertable(
        'bars',
        'time',
        chunk_time_interval => 86400000,
        if_not_exists => TRUE
    );
    `

	statusSQL := `
        CREATE TABLE IF NOT EXISTS statuses ( 
            time TIMESTAMPTZ,
            symbol VARCHAR,
            status_code VARCHAR,
            status_message VARCHAR,
            reason_code VARCHAR,
            reason_message VARCHAR,
            tape VARCHAR
        );
        SELECT create_hypertable(
            'statuses',
            'time',
            chunk_time_interval => 86400000,
            if_not_exists => TRUE
        );
    `

	quotesSQL := `
    CREATE TABLE IF NOT EXISTS quotes ( 
        time TIMESTAMPTZ,
        symbol VARCHAR,
        high DOUBLE PRECISION,
        low DOUBLE PRECISION
    );
    SELECT create_hypertable(
        'quotes',
        'time',
        chunk_time_interval => 86400000,
        if_not_exists => TRUE
    );
    `

	tradesSQL := `
    CREATE TABLE IF NOT EXISTS trades ( 
        time TIMESTAMPTZ,
        symbol VARCHAR,
        high DOUBLE PRECISION,
        low DOUBLE PRECISION,
        volume int
    );
    SELECT create_hypertable(
        'trades',
        'time',
        chunk_time_interval => 86400000,
        if_not_exists => TRUE
    );
    `

	ctx := context.Background()
	dbpool, err := pgxpool.Connect(ctx, util.Config.TSDBConnection)
	if err != nil {
		log.Fatal("cannot connect to TSDB", err)
	}
	log.Println("connected to TSDB")

	for _, sql := range []string{barSQL, statusSQL, quotesSQL, tradesSQL} {
		_, err := dbpool.Exec(ctx, sql)
		if err != nil {
			log.Fatal("error creating table", err, sql)
		}
		fmt.Println("created table")
	}
}
