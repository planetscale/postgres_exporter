// Copyright 2023 The Prometheus Authors
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

const longRunningTransactionsSubsystem = "long_running_transactions"

func init() {
	registerCollector(longRunningTransactionsSubsystem, defaultDisabled, NewPGLongRunningTransactionsCollector)
}

type PGLongRunningTransactionsCollector struct {
	log *slog.Logger
}

func NewPGLongRunningTransactionsCollector(config collectorConfig) (Collector, error) {
	return &PGLongRunningTransactionsCollector{log: config.logger}, nil
}

var (
	longRunningTransactionsCount = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, longRunningTransactionsSubsystem, "count"),
		"Number of transactions running longer than threshold",
		[]string{"threshold"},
		prometheus.Labels{},
	)

	longRunningTransactionsAgeInSeconds = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, longRunningTransactionsSubsystem, "oldest_timestamp_seconds"),
		"The current maximum transaction age in seconds",
		[]string{},
		prometheus.Labels{},
	)

	longRunningTransactionsQuery = `
		SELECT
			COUNT(*) FILTER (WHERE EXTRACT(EPOCH FROM clock_timestamp() - pg_stat_activity.xact_start) >= 60) AS count_60s,
			COUNT(*) FILTER (WHERE EXTRACT(EPOCH FROM clock_timestamp() - pg_stat_activity.xact_start) >= 300) AS count_300s,
			COUNT(*) FILTER (WHERE EXTRACT(EPOCH FROM clock_timestamp() - pg_stat_activity.xact_start) >= 600) AS count_600s,
			COUNT(*) FILTER (WHERE EXTRACT(EPOCH FROM clock_timestamp() - pg_stat_activity.xact_start) >= 1800) AS count_1800s,
			MAX(EXTRACT(EPOCH FROM clock_timestamp() - pg_stat_activity.xact_start)) AS oldest_timestamp_seconds
		FROM pg_catalog.pg_stat_activity
		WHERE state IS DISTINCT FROM 'idle'
		AND query NOT LIKE 'autovacuum:%'
		AND pg_stat_activity.xact_start IS NOT NULL;
	`
)

func (PGLongRunningTransactionsCollector) Update(ctx context.Context, instance *Instance, ch chan<- prometheus.Metric) error {
	db := instance.getDB()

	var count60s, count300s, count600s, count1800s float64
	var maxAge sql.NullFloat64

	err := db.QueryRowContext(ctx, longRunningTransactionsQuery).Scan(
		&count60s,
		&count300s,
		&count600s,
		&count1800s,
		&maxAge,
	)
	if err != nil {
		return err
	}

	// Emit count metrics with threshold labels
	ch <- prometheus.MustNewConstMetric(
		longRunningTransactionsCount,
		prometheus.GaugeValue,
		count60s,
		"60",
	)
	ch <- prometheus.MustNewConstMetric(
		longRunningTransactionsCount,
		prometheus.GaugeValue,
		count300s,
		"300",
	)
	ch <- prometheus.MustNewConstMetric(
		longRunningTransactionsCount,
		prometheus.GaugeValue,
		count600s,
		"600",
	)
	ch <- prometheus.MustNewConstMetric(
		longRunningTransactionsCount,
		prometheus.GaugeValue,
		count1800s,
		"1800",
	)

	// Emit max age metric
	ageValue := 0.0
	if maxAge.Valid {
		ageValue = maxAge.Float64
	}
	ch <- prometheus.MustNewConstMetric(
		longRunningTransactionsAgeInSeconds,
		prometheus.GaugeValue,
		ageValue,
	)

	return nil
}
