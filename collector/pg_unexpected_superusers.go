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

	"github.com/blang/semver/v4"
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
		[]string{"rolname", "access_type"}, nil,
	)

	// Roles that are expected to have superuser privileges.
	expectedSuperusers = map[string]struct{}{
		"pscale_admin": {},
	}

	pgUnexpectedSuperusersQuery = "SELECT rolname, 'direct'::pg_catalog.text AS access_type FROM pg_catalog.pg_roles WHERE rolsuper"

	pgUnexpectedSuperusersQueryPG16 = `WITH RECURSIVE superuser_chain AS (
    SELECT oid, rolname, 'direct'::pg_catalog.text AS access_type
    FROM pg_catalog.pg_roles WHERE rolsuper
    UNION
    SELECT r.oid, r.rolname, 'indirect'::pg_catalog.text AS access_type
    FROM pg_catalog.pg_roles r
    JOIN pg_catalog.pg_auth_members m ON m.member OPERATOR(pg_catalog.=) r.oid
    JOIN superuser_chain s ON m.roleid OPERATOR(pg_catalog.=) s.oid
    WHERE NOT r.rolsuper
        AND (m.set_option OPERATOR(pg_catalog.=) true OR m.admin_option OPERATOR(pg_catalog.=) true)
)
SELECT rolname, access_type FROM superuser_chain`
)

func (c PGUnexpectedSuperusersCollector) Update(ctx context.Context, instance *Instance, ch chan<- prometheus.Metric) error {
	query := pgUnexpectedSuperusersQuery
	if instance.version.GTE(semver.MustParse("16.0.0")) {
		query = pgUnexpectedSuperusersQueryPG16
	}

	db := instance.getDB()
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()

	var count float64
	for rows.Next() {
		var rolname sql.NullString
		var accessType sql.NullString
		if err := rows.Scan(&rolname, &accessType); err != nil {
			return err
		}

		if !rolname.Valid {
			continue
		}

		if _, ok := expectedSuperusers[rolname.String]; ok {
			continue
		}

		accessTypeLabel := "direct"
		if accessType.Valid {
			accessTypeLabel = accessType.String
		}

		count++
		ch <- prometheus.MustNewConstMetric(
			pgUnexpectedSuperuserDesc,
			prometheus.GaugeValue, 1, rolname.String, accessTypeLabel,
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
