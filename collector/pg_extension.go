// Copyright 2025 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package collector

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"math/rand"
	"slices"
	"strings"

	"github.com/alecthomas/kingpin/v2"
	"github.com/prometheus/client_golang/prometheus"
)

const extensionSubsystem = "extension"

var (
	extensionMaxDatabasesFlag     *int
	extensionIncludeDatabasesFlag *string
)

func init() {
	registerCollector(extensionSubsystem, defaultEnabled, NewPGExtensionCollector)

	extensionMaxDatabasesFlag = kingpin.Flag(
		"collector.extension.max-databases",
		"Maximum number of databases to scan for extensions per scrape. 0 = unlimited.",
	).Default("50").Int()

	extensionIncludeDatabasesFlag = kingpin.Flag(
		"collector.extension.include-databases",
		"Comma-separated list of databases to always scan for extensions (priority).",
	).Default("").String()
}

type PGExtensionCollector struct {
	log               *slog.Logger
	excludedDatabases []string
	maxDatabases      int
	includeDatabases  []string
}

func NewPGExtensionCollector(config collectorConfig) (Collector, error) {
	exclude := config.excludeDatabases
	if exclude == nil {
		exclude = []string{}
	}

	// Parse include databases from comma-separated flag
	var include []string
	if *extensionIncludeDatabasesFlag != "" {
		for _, db := range strings.Split(*extensionIncludeDatabasesFlag, ",") {
			db = strings.TrimSpace(db)
			if db != "" {
				include = append(include, db)
			}
		}
	}

	return &PGExtensionCollector{
		log:               config.logger,
		excludedDatabases: exclude,
		maxDatabases:      *extensionMaxDatabasesFlag,
		includeDatabases:  include,
	}, nil
}

var (
	pgExtensionInfoDesc = prometheus.NewDesc(
		prometheus.BuildFQName(
			namespace,
			extensionSubsystem,
			"info",
		),
		"Installed PostgreSQL extensions",
		[]string{"datname", "extname", "extversion"}, nil,
	)

	pgExtensionDatabasesDiscoveredDesc = prometheus.NewDesc(
		prometheus.BuildFQName(
			namespace,
			extensionSubsystem,
			"databases_discovered",
		),
		"Total number of connectable databases found",
		nil, nil,
	)

	pgExtensionDatabasesScannedDesc = prometheus.NewDesc(
		prometheus.BuildFQName(
			namespace,
			extensionSubsystem,
			"databases_scanned",
		),
		"Number of databases scanned for extensions this scrape",
		nil, nil,
	)

	// Query to list connectable, non-template databases
	pgExtensionDatabaseListQuery = `SELECT datname FROM pg_database
                                    WHERE datallowconn = true
                                    AND datistemplate = false`

	// Query to get extensions in current database
	pgExtensionQuery = `SELECT extname, extversion FROM pg_extension`
)

// Update implements Collector and exposes installed extensions across all databases.
func (c *PGExtensionCollector) Update(ctx context.Context, instance *Instance, ch chan<- prometheus.Metric) error {
	db := instance.getDB()

	// Get list of all databases
	databases, err := c.getDatabases(ctx, db)
	if err != nil {
		return fmt.Errorf("failed to query database list: %w", err)
	}

	// Filter out excluded databases
	var eligibleDatabases []string
	for _, dbName := range databases {
		if slices.Contains(c.excludedDatabases, dbName) {
			c.log.Debug("skipping excluded database", "database", dbName)
			continue
		}
		eligibleDatabases = append(eligibleDatabases, dbName)
	}

	// Emit discovered count (after exclusions)
	ch <- prometheus.MustNewConstMetric(
		pgExtensionDatabasesDiscoveredDesc,
		prometheus.GaugeValue,
		float64(len(eligibleDatabases)),
	)

	if len(eligibleDatabases) == 0 {
		c.log.Debug("no databases to query for extensions")
		ch <- prometheus.MustNewConstMetric(
			pgExtensionDatabasesScannedDesc,
			prometheus.GaugeValue,
			0,
		)
		return nil
	}

	// Build target list with priority databases first, then random sample of others
	targetDatabases := c.selectDatabases(eligibleDatabases)

	// Emit scanned count
	ch <- prometheus.MustNewConstMetric(
		pgExtensionDatabasesScannedDesc,
		prometheus.GaugeValue,
		float64(len(targetDatabases)),
	)

	// Query each database for extensions
	for _, dbName := range targetDatabases {
		if err := c.collectExtensionsForDatabase(ctx, ch, instance, dbName); err != nil {
			// Log and continue - don't fail entire collection for one database
			c.log.Warn("failed to collect extensions for database",
				"database", dbName, "err", err)
			continue
		}
	}

	return nil
}

// selectDatabases returns databases to scan: include-list first (guaranteed), then random others up to limit.
func (c *PGExtensionCollector) selectDatabases(eligible []string) []string {
	// If no limit, return all eligible
	if c.maxDatabases <= 0 {
		return eligible
	}

	// Separate include-list databases from others
	var priorityDBs []string
	var otherDBs []string

	eligibleSet := make(map[string]bool)
	for _, db := range eligible {
		eligibleSet[db] = true
	}

	// Add priority databases that exist in eligible set
	for _, db := range c.includeDatabases {
		if eligibleSet[db] {
			priorityDBs = append(priorityDBs, db)
			delete(eligibleSet, db) // Remove from set so we don't add it again
		}
	}

	// Collect remaining databases
	for _, db := range eligible {
		if eligibleSet[db] {
			otherDBs = append(otherDBs, db)
		}
	}

	// Shuffle the other databases randomly
	rand.Shuffle(len(otherDBs), func(i, j int) {
		otherDBs[i], otherDBs[j] = otherDBs[j], otherDBs[i]
	})

	// Build final list: priority first, then others, up to limit
	result := priorityDBs

	// Calculate how many more we can add
	remaining := c.maxDatabases - len(result)
	if remaining > 0 && len(otherDBs) > 0 {
		if remaining > len(otherDBs) {
			remaining = len(otherDBs)
		}
		result = append(result, otherDBs[:remaining]...)
	}

	return result
}

// getDatabases queries the list of connectable databases
func (c *PGExtensionCollector) getDatabases(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx, pgExtensionDatabaseListQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		var datname string
		if err := rows.Scan(&datname); err != nil {
			return nil, err
		}
		databases = append(databases, datname)
	}

	return databases, rows.Err()
}

// collectExtensionsForDatabase connects to a specific database and collects its extensions
func (c *PGExtensionCollector) collectExtensionsForDatabase(
	ctx context.Context,
	ch chan<- prometheus.Metric,
	instance *Instance,
	dbName string,
) error {
	// Connect to the target database
	db, err := instance.ConnectToDatabase(dbName)
	if err != nil {
		return err
	}
	defer db.Close()

	// Query extensions
	rows, err := db.QueryContext(ctx, pgExtensionQuery)
	if err != nil {
		return fmt.Errorf("failed to query extensions in database %s: %w", dbName, err)
	}
	defer rows.Close()

	for rows.Next() {
		var extname, extversion string
		if err := rows.Scan(&extname, &extversion); err != nil {
			return fmt.Errorf("failed to scan extension row: %w", err)
		}

		ch <- prometheus.MustNewConstMetric(
			pgExtensionInfoDesc,
			prometheus.GaugeValue,
			1,
			dbName, extname, extversion,
		)
	}

	return rows.Err()
}
