package database

import (
	"context"
	"fmt"
	"log"
	"time"

	"jarvist/sftp-service/internal/config"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func NewDatabase(cfg config.Database) (*gorm.DB, error) {
	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s TimeZone=Asia/Jakarta",
		cfg.Host, cfg.Port, cfg.Username, cfg.Password, cfg.Database, cfg.SSLMode)

	gormConfig := &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
		NowFunc: func() time.Time {
			return config.NowJakarta()
		},
		DisableForeignKeyConstraintWhenMigrating: true,
		PrepareStmt:                              false,
	}

	var db *gorm.DB
	var err error
	maxRetries := 3

	for i := 1; i <= maxRetries; i++ {
		db, err = gorm.Open(postgres.New(postgres.Config{
			DSN:                  dsn,
			PreferSimpleProtocol: true,
		}), gormConfig)

		if err == nil {
			break
		}

		log.Printf("Database connection attempt %d/%d failed: %v", i, maxRetries, err)
		if i < maxRetries {
			time.Sleep(time.Duration(i) * 2 * time.Second)
		}
	}

	if err != nil {
		return nil, fmt.Errorf("database connection failed after %d attempts: %w", maxRetries, err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("failed to get database instance: %w", err)
	}

	sqlDB.Exec("SET timezone = 'Asia/Jakarta'")

	sqlDB.SetMaxOpenConns(50)
	sqlDB.SetMaxIdleConns(20)
	sqlDB.SetConnMaxLifetime(10 * time.Minute)
	sqlDB.SetConnMaxIdleTime(5 * time.Minute)

	sqlDB.Exec(fmt.Sprintf("SET statement_timeout = '%d'", int(cfg.LongTimeout.Milliseconds())))
	sqlDB.Exec("SET lock_timeout = '60s'")
	sqlDB.Exec("SET idle_in_transaction_session_timeout = '300s'")

	// Optimize for bulk queries
	sqlDB.Exec("SET work_mem = '256MB'")             // Increase work memory for sorting/grouping
	sqlDB.Exec("SET maintenance_work_mem = '512MB'") // For index operations
	sqlDB.Exec("SET effective_cache_size = '4GB'")   // Assume reasonable cache size

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := sqlDB.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("database ping failed: %w", err)
	}

	return db, nil
}

func CreateQueryContext(queryType string, cfg *config.Config) (context.Context, context.CancelFunc) {
	var timeout time.Duration

	switch queryType {
	case "update":
		timeout = 45 * time.Second
	case "bulk":
		timeout = cfg.Database.BulkTimeout
	case "long":
		timeout = cfg.Database.LongTimeout
	default:
		timeout = cfg.Database.DefaultTimeout
	}

	return context.WithTimeout(context.Background(), timeout)
}
