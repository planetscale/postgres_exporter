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
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/smartystreets/goconvey/convey"
)

func TestPostgresBinariesCollector(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}
	defer db.Close()

	inst := &Instance{db: db}

	// All three functions exist
	mock.ExpectQuery(`SELECT EXISTS \(SELECT 1 FROM pg_proc WHERE proname = \$1\)`).
		WithArgs(pgPscaleUtilsBuildTimestampFunc).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectQuery(`SELECT pg_pscale_utils_build_unix_timestamp\(\)`).
		WillReturnRows(sqlmock.NewRows([]string{"ts"}).AddRow(1700000001))

	mock.ExpectQuery(`SELECT EXISTS \(SELECT 1 FROM pg_proc WHERE proname = \$1\)`).
		WithArgs(pgReadonlyBuildTimestampFunc).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectQuery(`SELECT pg_readonly_build_unix_timestamp\(\)`).
		WillReturnRows(sqlmock.NewRows([]string{"ts"}).AddRow(1700000002))

	mock.ExpectQuery(`SELECT EXISTS \(SELECT 1 FROM pg_proc WHERE proname = \$1\)`).
		WithArgs(pginsightsBuildTimestampFunc).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectQuery(`SELECT pginsights_build_unix_timestamp\(\)`).
		WillReturnRows(sqlmock.NewRows([]string{"ts"}).AddRow(1700000003))

	ch := make(chan prometheus.Metric)
	errCh := make(chan error, 1)
	go func() {
		defer close(ch)
		c := PostgresBinariesCollector{}
		errCh <- c.Update(context.Background(), inst, ch)
	}()

	// Collect all metrics from the channel
	var metrics []MetricResult
	for m := range ch {
		metrics = append(metrics, readMetric(m))
	}

	// Check for errors from the goroutine
	if err := <-errCh; err != nil {
		t.Errorf("Error calling PostgresBinariesCollector.Update: %s", err)
	}

	expected := []MetricResult{
		{labels: labelMap{}, value: 1700000001, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{}, value: 1700000002, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{}, value: 1700000003, metricType: dto.MetricType_GAUGE},
	}
	convey.Convey("Metrics comparison", t, func() {
		convey.So(metrics, convey.ShouldResemble, expected)
	})
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestPostgresBinariesCollectorFunctionNotExists(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}
	defer db.Close()

	inst := &Instance{db: db}

	// First function exists, others don't
	mock.ExpectQuery(`SELECT EXISTS \(SELECT 1 FROM pg_proc WHERE proname = \$1\)`).
		WithArgs(pgPscaleUtilsBuildTimestampFunc).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectQuery(`SELECT pg_pscale_utils_build_unix_timestamp\(\)`).
		WillReturnRows(sqlmock.NewRows([]string{"ts"}).AddRow(1700000001))

	mock.ExpectQuery(`SELECT EXISTS \(SELECT 1 FROM pg_proc WHERE proname = \$1\)`).
		WithArgs(pgReadonlyBuildTimestampFunc).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))

	mock.ExpectQuery(`SELECT EXISTS \(SELECT 1 FROM pg_proc WHERE proname = \$1\)`).
		WithArgs(pginsightsBuildTimestampFunc).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))

	ch := make(chan prometheus.Metric)
	errCh := make(chan error, 1)
	go func() {
		defer close(ch)
		c := PostgresBinariesCollector{}
		errCh <- c.Update(context.Background(), inst, ch)
	}()

	// Collect all metrics from the channel
	var metrics []MetricResult
	for m := range ch {
		metrics = append(metrics, readMetric(m))
	}

	// Check for errors from the goroutine
	if err := <-errCh; err != nil {
		t.Errorf("Error calling PostgresBinariesCollector.Update: %s", err)
	}

	// Only one metric should be emitted
	expected := []MetricResult{
		{labels: labelMap{}, value: 1700000001, metricType: dto.MetricType_GAUGE},
	}
	convey.Convey("Metrics comparison", t, func() {
		convey.So(metrics, convey.ShouldResemble, expected)
	})
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestPostgresBinariesCollectorNoFunctionsExist(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}
	defer db.Close()

	inst := &Instance{db: db}

	// No functions exist
	mock.ExpectQuery(`SELECT EXISTS \(SELECT 1 FROM pg_proc WHERE proname = \$1\)`).
		WithArgs(pgPscaleUtilsBuildTimestampFunc).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))

	mock.ExpectQuery(`SELECT EXISTS \(SELECT 1 FROM pg_proc WHERE proname = \$1\)`).
		WithArgs(pgReadonlyBuildTimestampFunc).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))

	mock.ExpectQuery(`SELECT EXISTS \(SELECT 1 FROM pg_proc WHERE proname = \$1\)`).
		WithArgs(pginsightsBuildTimestampFunc).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))

	ch := make(chan prometheus.Metric)
	errCh := make(chan error, 1)
	go func() {
		defer close(ch)
		c := PostgresBinariesCollector{}
		errCh <- c.Update(context.Background(), inst, ch)
	}()

	// Collect all metrics from the channel (should be none)
	var metrics []MetricResult
	for m := range ch {
		metrics = append(metrics, readMetric(m))
	}

	// Check that we got ErrNoData
	if err := <-errCh; err != ErrNoData {
		t.Errorf("Expected ErrNoData, got: %v", err)
	}

	// No metrics should be emitted
	convey.Convey("No metrics emitted", t, func() {
		convey.So(metrics, convey.ShouldBeEmpty)
	})
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestPostgresBinariesCollectorConnectionError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}
	defer db.Close()

	inst := &Instance{db: db}

	// Connection error on first query
	mock.ExpectQuery(`SELECT EXISTS \(SELECT 1 FROM pg_proc WHERE proname = \$1\)`).
		WithArgs(pgPscaleUtilsBuildTimestampFunc).
		WillReturnError(fmt.Errorf("connection refused"))

	ch := make(chan prometheus.Metric)
	errCh := make(chan error, 1)
	go func() {
		defer close(ch)
		c := PostgresBinariesCollector{}
		errCh <- c.Update(context.Background(), inst, ch)
	}()

	// Collect all metrics from the channel (should be none)
	var metrics []MetricResult
	for m := range ch {
		metrics = append(metrics, readMetric(m))
	}

	// Check that we got an error
	if err := <-errCh; err == nil {
		t.Errorf("Expected error, got nil")
	}

	// No metrics should be emitted
	convey.Convey("No metrics emitted on error", t, func() {
		convey.So(metrics, convey.ShouldBeEmpty)
	})
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}
