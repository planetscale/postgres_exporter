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
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/blang/semver/v4"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/promslog"
	"github.com/smartystreets/goconvey/convey"
)

func newTestSyncStandbySlotsCollector() *PGSynchronizedStandbySlotsCollector {
	return &PGSynchronizedStandbySlotsCollector{log: promslog.NewNopLogger()}
}

func TestPGSynchronizedStandbySlotsBeforePG17(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub database connection: %s", err)
	}
	defer db.Close()

	inst := &Instance{db: db, version: semver.MustParse("16.4.0")}

	ch := make(chan prometheus.Metric)
	go func() {
		defer close(ch)
		c := newTestSyncStandbySlotsCollector()
		if err := c.Update(context.Background(), inst, ch); err != nil {
			t.Errorf("Error calling Update: %s", err)
		}
	}()

	// No metrics should be emitted for PG < 17
	for range ch {
		t.Error("Expected no metrics for PG < 17, but got one")
	}
}

func TestPGSynchronizedStandbySlotsAllValid(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub database connection: %s", err)
	}
	defer db.Close()

	inst := &Instance{db: db, version: semver.MustParse("17.0.0")}

	mock.ExpectQuery(sanitizeQuery(synchronizedStandbySlotsQuery)).
		WillReturnRows(sqlmock.NewRows([]string{"invalid_count"}).AddRow(0))

	ch := make(chan prometheus.Metric)
	go func() {
		defer close(ch)
		c := newTestSyncStandbySlotsCollector()
		if err := c.Update(context.Background(), inst, ch); err != nil {
			t.Errorf("Error calling Update: %s", err)
		}
	}()

	expected := []MetricResult{
		{labels: labelMap{}, value: 0, metricType: dto.MetricType_GAUGE},
	}

	convey.Convey("All slots valid", t, func() {
		for _, expect := range expected {
			m := readMetric(<-ch)
			convey.So(expect, convey.ShouldResemble, m)
		}
	})
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestPGSynchronizedStandbySlotsSomeInvalid(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub database connection: %s", err)
	}
	defer db.Close()

	inst := &Instance{db: db, version: semver.MustParse("17.0.0")}

	mock.ExpectQuery(sanitizeQuery(synchronizedStandbySlotsQuery)).
		WillReturnRows(sqlmock.NewRows([]string{"invalid_count"}).AddRow(2))

	ch := make(chan prometheus.Metric)
	go func() {
		defer close(ch)
		c := newTestSyncStandbySlotsCollector()
		if err := c.Update(context.Background(), inst, ch); err != nil {
			t.Errorf("Error calling Update: %s", err)
		}
	}()

	expected := []MetricResult{
		{labels: labelMap{}, value: 2, metricType: dto.MetricType_GAUGE},
	}

	convey.Convey("Some slots invalid", t, func() {
		for _, expect := range expected {
			m := readMetric(<-ch)
			convey.So(expect, convey.ShouldResemble, m)
		}
	})
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestPGSynchronizedStandbySlotsEmptyGUC(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub database connection: %s", err)
	}
	defer db.Close()

	inst := &Instance{db: db, version: semver.MustParse("17.0.0")}

	mock.ExpectQuery(sanitizeQuery(synchronizedStandbySlotsQuery)).
		WillReturnRows(sqlmock.NewRows([]string{"invalid_count"}).AddRow(0))

	ch := make(chan prometheus.Metric)
	go func() {
		defer close(ch)
		c := newTestSyncStandbySlotsCollector()
		if err := c.Update(context.Background(), inst, ch); err != nil {
			t.Errorf("Error calling Update: %s", err)
		}
	}()

	expected := []MetricResult{
		{labels: labelMap{}, value: 0, metricType: dto.MetricType_GAUGE},
	}

	convey.Convey("Empty GUC emits 0", t, func() {
		for _, expect := range expected {
			m := readMetric(<-ch)
			convey.So(expect, convey.ShouldResemble, m)
		}
	})
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}
