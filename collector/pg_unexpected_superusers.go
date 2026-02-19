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

	"github.com/prometheus/client_golang/prometheus"
)

const unexpectedSuperusersSubsystem = "unexpected_superusers"

func init() {
	registerCollector(unexpectedSuperusersSubsystem, defaultEnabled, NewPGUnexpectedSuperusersCollector)
}

type PGUnexpectedSuperusersCollector struct {
	log *slog.Logger
}

func NewPGUnexpectedSuperusersCollector(config collectorConfig) (Collector, error) {
	return &PGUnexpectedSuperusersCollector{
		log: config.logger,
	}, nil
}

var (
	pgUnexpectedSuperusersDesc = prometheus.NewDesc(
		prometheus.BuildFQName(
			namespace,
			unexpectedSuperusersSubsystem,
			"count",
		),
		"Number of superuser roles that are not in the expected superuser list",
		[]string{}, nil,
	)

	pgUnexpectedSuperuserDesc = prometheus.NewDesc(
		prometheus.BuildFQName(
			namespace,
			unexpectedSuperusersSubsystem,
			"role",
		),
		"Unexpected superuser role (value is always 1)",
		[]string{"rolname"}, nil,
	)

	// Roles that are expected to have superuser privileges.
	expectedSuperusers = map[string]struct{}{
		"pscale_admin": {},
	}

	pgUnexpectedSuperusersQuery = "SELECT rolname FROM pg_roles WHERE rolsuper"
)

func (c PGUnexpectedSuperusersCollector) Update(ctx context.Context, instance *Instance, ch chan<- prometheus.Metric) error {
	db := instance.getDB()
	rows, err := db.QueryContext(ctx, pgUnexpectedSuperusersQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	var count float64
	for rows.Next() {
		var rolname sql.NullString
		if err := rows.Scan(&rolname); err != nil {
			return err
		}

		if !rolname.Valid {
			continue
		}

		if _, ok := expectedSuperusers[rolname.String]; ok {
			continue
		}

		count++
		ch <- prometheus.MustNewConstMetric(
			pgUnexpectedSuperuserDesc,
			prometheus.GaugeValue, 1, rolname.String,
		)
	}

	if err := rows.Err(); err != nil {
		return err
	}

	ch <- prometheus.MustNewConstMetric(
		pgUnexpectedSuperusersDesc,
		prometheus.GaugeValue, count,
	)

	return nil
}
