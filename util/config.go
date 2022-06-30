package util

import (
	"log"

	"github.com/spf13/viper"
)

type Envars struct {
	RedisEndpoint           string `mapstructure:"REDIS_ENDPOINT"`
	RedisStreamStart        string `mapstructure:"REDIS_STREAM_START"`
	RedisStreamCount        int64  `mapstructure:"REDIS_STREAM_COUNT"`
	TimescaleDBConnection   string `mapstructure:"TIMESCALEDB_CONNECTION"`
	TimescaleDBBatchSize    int    `mapstructure:"TIMESCALEDB_BATCH_SIZE"`
	TimescaleDBBatchTimeout int    `mapstructure:"TIMESCALEDB_BATCH_TIMEOUT"`
	TimescaleDBWorkers      int    `mapstructure:"TIMESCALEDB_WORKERS"`
}

var Config Envars

func init() {
	config, err := loadConfig(".")
	if err != nil {
		log.Fatal("could not load config", err)
	}
	Config = config

}

// LoadConfig loads app.env if it exists and sets envars
func loadConfig(path string) (envars Envars, err error) {
	viper.AddConfigPath(path)
	viper.SetConfigName("app")
	viper.SetConfigType("env")
	viper.AutomaticEnv()

	err = viper.ReadInConfig()
	if err != nil {
		return
	}

	err = viper.Unmarshal(&envars)
	return
}
