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
	"log/slog"

	"github.com/blang/semver/v4"
	"github.com/prometheus/client_golang/prometheus"
)

const synchronizedStandbySlotsSubsystem = "synchronized_standby_slots"

func init() {
	registerCollector(synchronizedStandbySlotsSubsystem, defaultEnabled, NewPGSynchronizedStandbySlotsCollector)
}

type PGSynchronizedStandbySlotsCollector struct {
	log *slog.Logger
}

func NewPGSynchronizedStandbySlotsCollector(config collectorConfig) (Collector, error) {
	return &PGSynchronizedStandbySlotsCollector{log: config.logger}, nil
}

var (
	synchronizedStandbySlotsInvalidDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, synchronizedStandbySlotsSubsystem, "invalid"),
		"Number of slots listed in synchronized_standby_slots that do not exist as physical replication slots. Non-zero means logical replication is blocked.",
		[]string{},
		nil,
	)

	synchronizedStandbySlotsQuery = `
SELECT count(*) AS invalid_count
FROM unnest(string_to_array(
  (SELECT setting FROM pg_settings WHERE name = 'synchronized_standby_slots'),
  ','
)) AS configured(slot_name)
WHERE trim(configured.slot_name) != ''
  AND NOT EXISTS(
    SELECT 1 FROM pg_replication_slots s
    WHERE s.slot_name = trim(configured.slot_name)
      AND s.slot_type = 'physical'
  )
`
)

func (c *PGSynchronizedStandbySlotsCollector) Update(ctx context.Context, instance *Instance, ch chan<- prometheus.Metric) error {
	if instance.version.LT(semver.MustParse("17.0.0")) {
		c.log.Debug("synchronized_standby_slots collector is not available on PostgreSQL < 17, skipping")
		return nil
	}

	db := instance.getDB()

	var invalidCount sql.NullInt64
	if err := db.QueryRowContext(ctx, synchronizedStandbySlotsQuery).Scan(&invalidCount); err != nil {
		return err
	}

	value := 0.0
	if invalidCount.Valid {
		value = float64(invalidCount.Int64)
	}

	ch <- prometheus.MustNewConstMetric(
		synchronizedStandbySlotsInvalidDesc,
		prometheus.GaugeValue,
		value,
	)

	return nil
}
