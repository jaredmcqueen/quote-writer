package util

import (
	"log"

	"github.com/spf13/viper"
)

type Envars struct {
	RedisStreamsCount    int64  `mapstructure:"REDIS_STREAMS_COUNT"`
	RedisStreamsEndpoint string `mapstructure:"REDIS_STREAMS_ENDPOINT"`
	RedisStreamsNames    string `mapstructure:"REDIS_STREAMS_NAMES"`
	RedisStreamsStart    string `mapstructure:"REDIS_STREAMS_START"`

	PostgreSQLEnabled  bool   `mapstructure:"POSTGRESQL_ENABLED"`
	PostgreSQLEndpoint string `mapstructure:"POSTGRESQL_ENDPOINT"`

	RedisTSEnabled  bool   `mapstructure:"REDIS_TS_ENABLED"`
	RedisTSEndpoint string `mapstructure:"REDIS_TS_ENDPOINT"`

	BatchSize    int `mapstructure:"BATCH_SIZE"`
	BatchTimeout int `mapstructure:"BATCH_TIMEOUT"`
	CacheSize    int `mapstructure:"CACHE_SIZE"`
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
