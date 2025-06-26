package config

import (
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

const (
	TimezoneJakarta = "Asia/Jakarta"

	DefaultQueryTimeout = 60 * time.Second
	BulkQueryTimeout    = 300 * time.Second
	LongQueryTimeout    = 600 * time.Second

	DBConnectTimeout = 30 * time.Second
	DBMaxIdleTime    = 2 * time.Minute
	DBMaxLifetime    = 5 * time.Minute
)

type Config struct {
	Database    Database
	LocalPath   string
	LogPath     string
	GRPCPort    string
	NATS        NATSConfig
	Environment string
}

type Database struct {
	Host     string
	Port     string
	Database string
	Username string
	Password string
	SSLMode  string
	Debug    bool

	DefaultTimeout time.Duration
	BulkTimeout    time.Duration
	LongTimeout    time.Duration
}

type NATSConfig struct {
	URL            string `json:"url"`
	Username       string `json:"username"`
	Password       string `json:"password"`
	MaxReconnects  int    `json:"max_reconnects"`
	ReconnectWait  int    `json:"reconnect_wait"`
	ConnectTimeout int    `json:"connect_timeout"`
	StreamName     string `json:"stream_name"`
	MaxMessages    int64  `json:"max_messages"`
	MaxBytes       int64  `json:"max_bytes"`
	MaxAge         int    `json:"max_age_hours"`
}

type LateDataScheduleConfig struct {
	DailyCheckHours        []int
	ThirtyMinCheckMinute   int
	DailyCheckMinute       int
	ThirtyMinLookBackHours int
	HistoricalLookbackDays int
	EnableLateDataCheck    bool
}

func Load() *Config {
	godotenv.Load()

	return &Config{
		Environment: getEnv("ENVIRONMENT", "production"),
		Database: Database{
			Host:     getEnv("DB_HOST", "localhost"),
			Port:     getEnv("DB_PORT", "5432"),
			Database: getEnv("DB_DATABASE", "postgres"),
			Username: getEnv("DB_USERNAME", "postgres"),
			Password: getEnv("DB_PASSWORD", ""),
			SSLMode:  getEnv("DB_SSL_MODE", "disable"),
			Debug:    getBoolEnv("DB_DEBUG", false),

			DefaultTimeout: getDurationEnv("DB_DEFAULT_TIMEOUT", DefaultQueryTimeout),
			BulkTimeout:    getDurationEnv("DB_BULK_TIMEOUT", BulkQueryTimeout),
			LongTimeout:    getDurationEnv("DB_LONG_TIMEOUT", LongQueryTimeout),
		},
		NATS: NATSConfig{
			URL:            getEnv("NATS_URL", "nats://localhost:4222"),
			Username:       getEnv("NATS_USERNAME", ""),
			Password:       getEnv("NATS_PASSWORD", ""),
			MaxReconnects:  getIntEnv("NATS_MAX_RECONNECTS", -1),
			ReconnectWait:  getIntEnv("NATS_RECONNECT_WAIT", 5),
			ConnectTimeout: getIntEnv("NATS_CONNECT_TIMEOUT", 30),
			StreamName:     getEnv("NATS_STREAM_NAME", "JARVIST_SYNC"),
			MaxMessages:    getInt64Env("NATS_MAX_MESSAGES", 1000000),
			MaxBytes:       getInt64Env("NATS_MAX_BYTES", 2147483648), // 2GB
			MaxAge:         getIntEnv("NATS_MAX_AGE_HOURS", 24),
		},
		LocalPath: getEnv("LOCAL_PATH", "./temp"),
		LogPath:   getEnv("LOG_PATH", "./logs"),
		GRPCPort:  getEnv("SFTP_SERVICE_PORT", "9081"),
	}
}

func GetLateDataScheduleConfig() LateDataScheduleConfig {

	defaultHours := []int{1, 8, 16}

	if hoursStr := getEnv("DAILY_LATE_CHECK_HOURS", ""); hoursStr != "" {
		var hours []int
		for _, hourStr := range strings.Split(hoursStr, ",") {
			if hour, err := strconv.Atoi(strings.TrimSpace(hourStr)); err == nil {
				if hour >= 0 && hour <= 23 {
					hours = append(hours, hour)
				}
			}
		}
		if len(hours) > 0 {
			defaultHours = hours
		}
	}

	return LateDataScheduleConfig{
		DailyCheckHours:        defaultHours,
		ThirtyMinCheckMinute:   getIntEnv("THIRTY_MIN_LATE_CHECK_MINUTE", 35),
		DailyCheckMinute:       getIntEnv("DAILY_LATE_CHECK_MINUTE", 5),
		ThirtyMinLookBackHours: getIntEnv("THIRTY_MIN_LOOKBACK_HOURS", 1),
		HistoricalLookbackDays: getIntEnv("HISTORICAL_LOOKBACK_DAYS", 3),
		EnableLateDataCheck:    getBoolEnv("ENABLE_LATE_DATA_CHECK", true),
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getInt64Env(key string, defaultValue int64) int64 {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}

	int64Value, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return defaultValue
	}

	return int64Value
}

func getBoolEnv(key string, defaultValue bool) bool {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}

	boolValue, err := strconv.ParseBool(value)
	if err != nil {
		return defaultValue
	}

	return boolValue
}

func getIntEnv(key string, defaultValue int) int {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}

	intValue, err := strconv.Atoi(value)
	if err != nil {
		return defaultValue
	}

	return intValue
}

func getDurationEnv(key string, defaultValue time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}

	duration, err := time.ParseDuration(value)
	if err != nil {
		log.Printf("Invalid duration for %s: %s, using default: %v", key, value, defaultValue)
		return defaultValue
	}

	return duration
}

func GetJakartaTimezone() *time.Location {
	loc, err := time.LoadLocation(TimezoneJakarta)
	if err != nil {
		return time.UTC
	}
	return loc
}

func GetEnvironment() string {
	return getEnv("ENVIRONMENT", "production")
}

func NowJakarta() time.Time {
	return time.Now().In(GetJakartaTimezone())
}

type TimeRange struct {
	StartTime time.Time
	EndTime   time.Time
}

func BusinessHours(date time.Time) TimeRange {
	jakartaDate := date.In(GetJakartaTimezone())

	start := time.Date(jakartaDate.Year(), jakartaDate.Month(), jakartaDate.Day(), 8, 0, 0, 0, GetJakartaTimezone())
	end := time.Date(jakartaDate.Year(), jakartaDate.Month(), jakartaDate.Day(), 23, 30, 0, 0, GetJakartaTimezone())

	return TimeRange{StartTime: start, EndTime: end}
}
