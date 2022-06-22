package util

import "github.com/spf13/viper"

type Config struct {
	RedisEndpoint           string `mapstructure:"REDIS_ENDPOINT"`
	RedisStreamStart        string `mapstructure:"REDIS_STREAM_START"`
	RedisStreamCount        int64  `mapstructure:"REDIS_STREAM_COUNT"`
	TimescaleDBConnection   string `mapstructure:"TIMESCALEDB_CONNECTION"`
	TimescaleDBBatchSize    int    `mapstructure:"TIMESCALEDB_BATCH_SIZE"`
	TimescaleDBBatchTimeout int    `mapstructure:"TIMESCALEDB_BATCH_TIMEOUT"`
	TimescaleDBWorkers      int    `mapstructure:"TIMESCALEDB_WORKERS"`
}

// LoadConfig loads app.env if it exists and sets envars
func LoadConfig(path string) (config Config, err error) {
	viper.AddConfigPath(path)
	viper.SetConfigName("app")
	viper.SetConfigType("env")

	viper.AutomaticEnv()

	err = viper.ReadInConfig()
	if err != nil {
		return
	}

	err = viper.Unmarshal(&config)
	return
}
