package config

import (
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

const (
	TimezoneJakarta = "Asia/Jakarta"
)

type Config struct {
	Database  Database
	LocalPath string
	LogPath   string
	GRPCPort  string
}

type Database struct {
	Host     string
	Port     string
	Database string
	Username string
	Password string
	SSLMode  string
	Debug    bool
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
		Database: Database{
			Host:     getEnv("DB_HOST", "localhost"),
			Port:     getEnv("DB_PORT", "5432"),
			Database: getEnv("DB_DATABASE", "postgres"),
			Username: getEnv("DB_USERNAME", "postgres"),
			Password: getEnv("DB_PASSWORD", ""),
			SSLMode:  getEnv("DB_SSL_MODE", "disable"),
			Debug:    getBoolEnv("DB_DEBUG", false),
		},
		LocalPath: getEnv("LOCAL_PATH", "./temp"),
		LogPath:   getEnv("LOG_PATH", "./logs"),
		GRPCPort:  getEnv("SFTP_SERVICE_PORT", "9082"),
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

func GetJakartaTimezone() *time.Location {
	loc, err := time.LoadLocation(TimezoneJakarta)
	if err != nil {
		return time.UTC
	}
	return loc
}

func NowJakarta() time.Time {
	return time.Now().In(GetJakartaTimezone())
}
