package main

import (
	"context"
	"fmt"
	"os"
	"strings"
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
	configPath := cast.ToString(c.Vals["config"])
	if configPath == "" {
		return false, g.Error("Configuration file path is required")
	}

	cfg, err := readCleanPartitionsConfig(configPath)
	if err != nil {
		return false, g.Error(err, "failure reading clean partitions configuration")
	}

	err = runCleanPartitions(cfg)
	if err != nil {
		return false, g.Error(err, "failure running clean partitions")
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
	conn, err := database.NewConn(cfg.TimeplusdConn)
	if err != nil {
		return g.Error(err, "Error connecting to timeplus database")
	}
	defer conn.Close()

	today := time.Now().Format("20060102")
	datePatterns := generateDatePatterns(today)
	patternClauses := generatePatternClauses(datePatterns)
	joinedPatternClauses := strings.Join(patternClauses, " OR ")

	for _, table := range cfg.Tables {
		err := cleanTablePartitions(conn, table, today, joinedPatternClauses)
		if err != nil {
			g.Warn("Error cleaning partitions for table %s: %v", table, err)
		}
	}

	return nil
}

func generateDatePatterns(date string) []string {
	dateObj, err := time.Parse("20060102", date)
	if err != nil {
		g.Warn("Invalid date format: %s. Expected YYYYMMDD", date)
		return []string{date}
	}

	return []string{
		dateObj.Format("20060102"),   // YYYYMMDD
		dateObj.Format("2006-01-02"), // YYYY-MM-DD
		dateObj.Format("2006_01_02"), // YYYY_MM_DD
		dateObj.Format("2006/01/02"), // YYYY/MM/DD
	}
}

func generatePatternClauses(datePatterns []string) []string {
	patternClauses := make([]string, len(datePatterns))
	for i, pattern := range datePatterns {
		patternClauses[i] = fmt.Sprintf("partition LIKE '%%%s%%'", pattern)
	}
	return patternClauses
}

func cleanTablePartitions(conn database.Connection, table, date, joinedPatternClauses string) error {
	partitions, err := getPartitions(conn, table, joinedPatternClauses)
	if err != nil {
		return g.Error(err, "Error getting partitions for table: %s", table)
	}
	if len(partitions) > 0 {
		g.Info("Retrieved partitions %v for table: %s", partitions, table)
	} else {
		g.Info("No partitions like %s found for table: %s", date, table)
		return nil
	}

	for _, partition := range partitions {
		err := dropPartition(conn, table, partition)
		if err != nil {
			g.Error("Failed to drop partition %s from table %s: %v", partition, table, err)
		} else {
			g.Info("Successfully dropped partition %s from table %s", partition, table)
		}
		time.Sleep(1 * time.Second) // Delay between partition drops
	}

	g.Info("Finished cleaning partitions for table: %s", table)
	return nil
}

func getPartitions(conn database.Connection, table, joinedPatternClauses string) ([]string, error) {
	query := fmt.Sprintf(`
		SELECT DISTINCT partition
		FROM system.parts
		WHERE table = '%s'
		  AND (%s)
	`, table, joinedPatternClauses)

	data, err := conn.Query(query)
	if err != nil {
		return nil, g.Error(err, "Error querying partitions")
	}

	var partitions []string
	for _, row := range data.Rows {
		partitions = append(partitions, cast.ToString(row[0]))
	}

	return partitions, nil
}

func dropPartition(conn database.Connection, table, partition string) error {
	dropQuery := fmt.Sprintf("ALTER STREAM %s DROP PARTITION %s", table, partition)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := conn.ExecContext(ctx, dropQuery)
	if err != nil {
		return g.Error(err, "Error executing drop partition query")
	}
	return nil
}
