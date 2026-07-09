package db

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq"
)

var DB *sql.DB

func InitDB() error {
	connStr := "host=localhost port=5432 user=postgres password=postgres dbname=orderdb sslmode=disable"

	var err error
	DB, err = sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("failed to open database connection: %w", err)
	}

	DB.SetMaxOpenConns(25)
	DB.SetMaxIdleConns(10)
	DB.SetConnMaxLifetime(5 * time.Minute)

	if err = DB.Ping(); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}

	return nil
}

func CloseDB() {
	if DB != nil {
		DB.Close()
	}
}
