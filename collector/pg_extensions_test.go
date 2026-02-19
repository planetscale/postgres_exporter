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
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/smartystreets/goconvey/convey"
)

func TestPGExtensionsCollector(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}
	defer db.Close()

	inst := &Instance{db: db}

	db2, mock2, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}
	db3, mock3, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}

	mock.ExpectQuery(sanitizeQuery(pgExtensionsDatabasesQuery)).
		WillReturnRows(sqlmock.NewRows([]string{"datname"}).
			AddRow("db1").
			AddRow("db2"))

	// db1 extensions
	mock2.ExpectQuery(sanitizeQuery(pgExtensionsQuery)).
		WillReturnRows(sqlmock.NewRows([]string{"extname", "extversion"}).
			AddRow("pgcrypto", "1.3"))

	// db2 extensions
	mock3.ExpectQuery(sanitizeQuery(pgExtensionsQuery)).
		WillReturnRows(sqlmock.NewRows([]string{"extname", "extversion"}).
			AddRow("uuid-ossp", "1.1"))

	perDBs := []*sql.DB{db2, db3}
	var callIdx int
	c := &PGExtensionsCollector{
		excludedDatabases: []string{},
		connectDB: func(dsn string) (*sql.DB, error) {
			d := perDBs[callIdx]
			callIdx++
			return d, nil
		},
	}

	ch := make(chan prometheus.Metric)
	go func() {
		defer close(ch)
		if err := c.Update(context.Background(), inst, ch); err != nil {
			t.Errorf("Error calling PGExtensionsCollector.Update: %s", err)
		}
	}()

	// Metrics are emitted sorted by extname: pgcrypto before uuid-ossp
	expected := []MetricResult{
		{labels: labelMap{"extname": "pgcrypto", "extversion": "1.3"}, value: 1, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{"extname": "uuid-ossp", "extversion": "1.1"}, value: 1, metricType: dto.MetricType_GAUGE},
	}
	convey.Convey("Extensions from two databases", t, func() {
		for _, expect := range expected {
			m := readMetric(<-ch)
			convey.So(expect, convey.ShouldResemble, m)
		}
	})

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations on main db: %s", err)
	}
	if err := mock2.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations on db2: %s", err)
	}
	if err := mock3.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations on db3: %s", err)
	}
}

func TestPGExtensionsCollectorDeduplication(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}
	defer db.Close()

	inst := &Instance{db: db}

	db2, mock2, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}
	db3, mock3, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}

	mock.ExpectQuery(sanitizeQuery(pgExtensionsDatabasesQuery)).
		WillReturnRows(sqlmock.NewRows([]string{"datname"}).
			AddRow("db1").
			AddRow("db2"))

	// Both databases have the same extension at the same version
	mock2.ExpectQuery(sanitizeQuery(pgExtensionsQuery)).
		WillReturnRows(sqlmock.NewRows([]string{"extname", "extversion"}).
			AddRow("pgcrypto", "1.3"))

	mock3.ExpectQuery(sanitizeQuery(pgExtensionsQuery)).
		WillReturnRows(sqlmock.NewRows([]string{"extname", "extversion"}).
			AddRow("pgcrypto", "1.3"))

	perDBs := []*sql.DB{db2, db3}
	var callIdx int
	c := &PGExtensionsCollector{
		excludedDatabases: []string{},
		connectDB: func(dsn string) (*sql.DB, error) {
			d := perDBs[callIdx]
			callIdx++
			return d, nil
		},
	}

	ch := make(chan prometheus.Metric)
	go func() {
		defer close(ch)
		if err := c.Update(context.Background(), inst, ch); err != nil {
			t.Errorf("Error calling PGExtensionsCollector.Update: %s", err)
		}
	}()

	// Only one metric emitted despite extension appearing in both databases
	expected := []MetricResult{
		{labels: labelMap{"extname": "pgcrypto", "extversion": "1.3"}, value: 1, metricType: dto.MetricType_GAUGE},
	}
	convey.Convey("Deduplicated extension across databases", t, func() {
		for _, expect := range expected {
			m := readMetric(<-ch)
			convey.So(expect, convey.ShouldResemble, m)
		}
		// Channel should be closed with no further metrics
		_, open := <-ch
		convey.So(open, convey.ShouldBeFalse)
	})

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations on main db: %s", err)
	}
	if err := mock2.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations on db2: %s", err)
	}
	if err := mock3.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations on db3: %s", err)
	}
}

func TestPGExtensionsCollectorExcludedDatabase(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}
	defer db.Close()

	inst := &Instance{db: db}

	db2, mock2, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}

	mock.ExpectQuery(sanitizeQuery(pgExtensionsDatabasesQuery)).
		WillReturnRows(sqlmock.NewRows([]string{"datname"}).
			AddRow("db1").
			AddRow("db2"))

	// Only db1 is queried; db2 is excluded
	mock2.ExpectQuery(sanitizeQuery(pgExtensionsQuery)).
		WillReturnRows(sqlmock.NewRows([]string{"extname", "extversion"}).
			AddRow("pgcrypto", "1.3"))

	c := &PGExtensionsCollector{
		excludedDatabases: []string{"db2"},
		connectDB: func(dsn string) (*sql.DB, error) {
			return db2, nil
		},
	}

	ch := make(chan prometheus.Metric)
	go func() {
		defer close(ch)
		if err := c.Update(context.Background(), inst, ch); err != nil {
			t.Errorf("Error calling PGExtensionsCollector.Update: %s", err)
		}
	}()

	expected := []MetricResult{
		{labels: labelMap{"extname": "pgcrypto", "extversion": "1.3"}, value: 1, metricType: dto.MetricType_GAUGE},
	}
	convey.Convey("Excluded database is skipped", t, func() {
		for _, expect := range expected {
			m := readMetric(<-ch)
			convey.So(expect, convey.ShouldResemble, m)
		}
		// No further metrics
		_, open := <-ch
		convey.So(open, convey.ShouldBeFalse)
	})

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations on main db: %s", err)
	}
	if err := mock2.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations on db2: %s", err)
	}
}
