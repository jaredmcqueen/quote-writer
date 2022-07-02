package util

import (
	"log"

	"github.com/spf13/viper"
)

type Envars struct {
	RedisEndpoint    string `mapstructure:"REDIS_ENDPOINT"`
	RedisStreamCount int64  `mapstructure:"REDIS_STREAM_COUNT"`
	RedisStreamStart string `mapstructure:"REDIS_STREAM_START"`
	RedisStreamNames string `mapstructure:"REDIS_STREAM_NAMES"`

	TSDBBatchSize   int    `mapstructure:"TSDB_BATCH_SIZE"`
	TSDBConnection  string `mapstructure:"TSDB_CONNECTION"`
	TSDBWorkers     int    `mapstructure:"TSDB_WORKERS"`
	TSDBatchTimeout int    `mapstructure:"TSDB_BATCH_TIMEOUT"`
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
