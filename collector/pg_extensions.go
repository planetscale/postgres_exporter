// Copyright 2024 The Prometheus Authors
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
	"log/slog"
	"slices"
	"sort"

	"github.com/prometheus/client_golang/prometheus"
)

const extensionSubsystem = "extension"

func init() {
	registerCollector(extensionSubsystem, defaultEnabled, NewPGExtensionsCollector)
}

type PGExtensionsCollector struct {
	log               *slog.Logger
	excludedDatabases []string
	connectDB         func(dsn string) (*sql.DB, error)
}

func NewPGExtensionsCollector(config collectorConfig) (Collector, error) {
	exclude := config.excludeDatabases
	if exclude == nil {
		exclude = []string{}
	}
	return &PGExtensionsCollector{
		log:               config.logger,
		excludedDatabases: exclude,
		connectDB: func(dsn string) (*sql.DB, error) {
			db, err := sql.Open("postgres", dsn)
			if err != nil {
				return nil, err
			}
			db.SetMaxOpenConns(1)
			db.SetMaxIdleConns(1)
			return db, nil
		},
	}, nil
}

var (
	pgExtensionInstalledDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, extensionSubsystem, "installed"),
		"Installed PostgreSQL extension (value is always 1)",
		[]string{"extname", "extversion"}, nil,
	)

	pgExtensionsDatabasesQuery = "SELECT datname FROM pg_database WHERE datistemplate = false AND datallowconn = true ORDER BY datname"
	pgExtensionsQuery          = "SELECT extname, extversion FROM pg_catalog.pg_extension ORDER BY extname"
)

func (c *PGExtensionsCollector) Update(ctx context.Context, instance *Instance, ch chan<- prometheus.Metric) error {
	db := instance.getDB()
	rows, err := db.QueryContext(ctx, pgExtensionsDatabasesQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		var datname sql.NullString
		if err := rows.Scan(&datname); err != nil {
			return err
		}
		if !datname.Valid {
			continue
		}
		if slices.Contains(c.excludedDatabases, datname.String) {
			continue
		}
		databases = append(databases, datname.String)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	// Collect (extname -> extversion) across all databases, deduplicating by extname.
	extensions := make(map[string]string)

	for _, datname := range databases {
		dsn, err := instance.connectionStringForDB(datname)
		if err != nil {
			c.log.Warn("failed to build connection string for database", "datname", datname, "err", err)
			continue
		}
		dbConn, err := c.connectDB(dsn)
		if err != nil {
			c.log.Warn("failed to connect to database", "datname", datname, "err", err)
			continue
		}

		extRows, err := dbConn.QueryContext(ctx, pgExtensionsQuery)
		if err != nil {
			c.log.Warn("failed to query extensions in database", "datname", datname, "err", err)
			dbConn.Close()
			continue
		}

		for extRows.Next() {
			var extname, extversion sql.NullString
			if err := extRows.Scan(&extname, &extversion); err != nil {
				extRows.Close()
				dbConn.Close()
				return err
			}
			if !extname.Valid {
				continue
			}
			version := ""
			if extversion.Valid {
				version = extversion.String
			}
			extensions[extname.String] = version
		}
		extRows.Close()

		if err := extRows.Err(); err != nil {
			c.log.Warn("error iterating extension rows", "datname", datname, "err", err)
		}
		dbConn.Close()
	}

	// Emit metrics sorted by extname for deterministic output.
	extNames := make([]string, 0, len(extensions))
	for name := range extensions {
		extNames = append(extNames, name)
	}
	sort.Strings(extNames)

	for _, extname := range extNames {
		ch <- prometheus.MustNewConstMetric(
			pgExtensionInstalledDesc,
			prometheus.GaugeValue, 1,
			extname, extensions[extname],
		)
	}

	return nil
}
