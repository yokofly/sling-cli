package main

import (
	"fmt"
	"os"
	"time"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio/database"
	"github.com/spf13/cast"
	"gopkg.in/yaml.v2"
)

type CleanPartitionsConfig struct {
	TimeplusdConn string   `yaml:"timeplusd_conn"`
	Tables        []string `yaml:"tables"`
}

func processCleanPartitions(c *g.CliSC) (bool, error) {
	configPath := ""

	if v, ok := c.Vals["config"]; ok {
		configPath = cast.ToString(v)
	}

	if configPath == "" {
		return false, g.Error("Configuration file path is required")
	}

	cfg, err := readCleanPartitionsConfig(configPath)
	if err != nil {
		return false, g.Error(err, "failure reading clean partitions configuration")
	}

	err = runCleanPartitions(cfg)
	if err != nil {
		return false, g.Error(err, "failure running clean partitions (see docs @ https://docs.slingdata.io/sling-cli)")
	}

	return true, nil
}

func readCleanPartitionsConfig(configPath string) (*CleanPartitionsConfig, error) {
	configData, err := os.ReadFile(configPath)
	if err != nil {
		return nil, g.Error(err, "Error reading config file")
	}

	var config CleanPartitionsConfig
	err = yaml.Unmarshal(configData, &config)
	if err != nil {
		return nil, g.Error(err, "Error parsing config file")
	}

	return &config, nil
}

func runCleanPartitions(cfg *CleanPartitionsConfig) error {
	// Connect to TimePlusD
	conn, err := database.NewConn(cfg.TimeplusdConn)
	if err != nil {
		return g.Error(err, "Error connecting to TimePlusD")
	}
	defer conn.Close()

	// Get today's date
	today := time.Now().Format("20060102")

	// Clean partitions for each table
	for _, table := range cfg.Tables {
		err := cleanTablePartitions(conn, table, today)
		if err != nil {
			g.Warn("Error cleaning partitions for table %s: %v", table, err)
		}
	}

	return nil
}

func cleanTablePartitions(conn database.Connection, table, date string) error {
	// Query to find partitions matching today's date
	query := fmt.Sprintf(`
        SELECT partition
        FROM system.parts
        WHERE table = '%s'
          AND partition LIKE '%%%s%%'
    `, table, date)

	data, err := conn.Query(query)
	if err != nil {
		return g.Error(err, "Error querying partitions")
	}

	for _, row := range data.Rows {
		partition := cast.ToString(row[0])

		// Retry logic for dropping partition
		retryCount := 0
		maxRetries := 3
		for retryCount < maxRetries {
			err := dropPartitionWithTimeout(conn, table, partition)
			if err == nil {
				g.Info("Dropped partition %s from table %s", partition, table)
				break
			}

			retryCount++
			if retryCount < maxRetries {
				g.Warn("Failed to drop partition %s from table %s (attempt %d of %d): %v", partition, table, retryCount, maxRetries, err)
				time.Sleep(3 * time.Second)
			} else {
				g.Error("Failed to drop partition %s from table %s after %d attempts: %v", partition, table, maxRetries, err)
			}
		}

		time.Sleep(1 * time.Second)
	}

	return nil
}

func dropPartitionWithTimeout(conn database.Connection, table, partition string) error {
	dropQuery := fmt.Sprintf("ALTER STREAM %s DROP PARTITION %s", table, partition)

	// Create a channel to signal completion
	done := make(chan error, 1)

	// Execute the drop query in a goroutine
	go func() {
		_, err := conn.Exec(dropQuery)
		done <- err
	}()

	// Wait for the operation to complete or timeout
	select {
	case err := <-done:
		return err
	case <-time.After(30 * time.Second): // 30-second timeout
		return g.Error("Timeout while dropping partition %s from table %s", partition, table)
	}
}
