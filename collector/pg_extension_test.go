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
)

func TestModifyDSNDatabase(t *testing.T) {
	tests := []struct {
		name     string
		inputDSN string
		dbName   string
		expected string
		wantErr  bool
	}{
		{
			name:     "URI style postgres://",
			inputDSN: "postgres://user:pass@localhost:5432/originaldb?sslmode=disable",
			dbName:   "newdb",
			expected: "postgres://user:pass@localhost:5432/newdb?sslmode=disable",
		},
		{
			name:     "URI style postgresql://",
			inputDSN: "postgresql://user:pass@localhost:5432/originaldb",
			dbName:   "newdb",
			expected: "postgresql://user:pass@localhost:5432/newdb",
		},
		{
			name:     "URI style without database",
			inputDSN: "postgres://user:pass@localhost:5432/?sslmode=disable",
			dbName:   "newdb",
			expected: "postgres://user:pass@localhost:5432/newdb?sslmode=disable",
		},
		{
			name:     "key=value style without dbname",
			inputDSN: "host=localhost port=5432 user=postgres",
			dbName:   "newdb",
			expected: "host=localhost port=5432 user=postgres dbname=newdb",
		},
		{
			name:     "key=value style with existing dbname",
			inputDSN: "host=localhost port=5432 user=postgres dbname=olddb",
			dbName:   "newdb",
			expected: "host=localhost port=5432 user=postgres dbname=newdb",
		},
		{
			name:     "key=value style with dbname in middle",
			inputDSN: "host=localhost dbname=olddb port=5432",
			dbName:   "newdb",
			expected: "host=localhost port=5432 dbname=newdb",
		},
		{
			name:     "invalid DSN format",
			inputDSN: "not a valid dsn",
			dbName:   "newdb",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := modifyDSNDatabase(tt.inputDSN, tt.dbName)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error but got none")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if result != tt.expected {
				t.Errorf("got %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestPGExtensionCollector_getDatabases(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}
	defer db.Close()

	mock.ExpectQuery(sanitizeQuery(pgExtensionDatabaseListQuery)).
		WillReturnRows(sqlmock.NewRows([]string{"datname"}).
			AddRow("postgres").
			AddRow("myapp").
			AddRow("analytics"))

	c := &PGExtensionCollector{}
	databases, err := c.getDatabases(context.Background(), db)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	expected := []string{"postgres", "myapp", "analytics"}
	if len(databases) != len(expected) {
		t.Errorf("got %d databases, want %d", len(databases), len(expected))
	}
	for i, d := range databases {
		if d != expected[i] {
			t.Errorf("database[%d] = %q, want %q", i, d, expected[i])
		}
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %s", err)
	}
}

func TestPGExtensionCollector_getDatabasesEmpty(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}
	defer db.Close()

	mock.ExpectQuery(sanitizeQuery(pgExtensionDatabaseListQuery)).
		WillReturnRows(sqlmock.NewRows([]string{"datname"}))

	c := &PGExtensionCollector{}
	databases, err := c.getDatabases(context.Background(), db)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if len(databases) != 0 {
		t.Errorf("expected empty list, got %d databases", len(databases))
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %s", err)
	}
}

func TestPGExtensionCollector_selectDatabases_NoLimit(t *testing.T) {
	c := &PGExtensionCollector{
		maxDatabases:     0, // No limit
		includeDatabases: []string{},
	}

	eligible := []string{"db1", "db2", "db3", "db4", "db5"}
	result := c.selectDatabases(eligible)

	if len(result) != len(eligible) {
		t.Errorf("expected all %d databases, got %d", len(eligible), len(result))
	}
}

func TestPGExtensionCollector_selectDatabases_WithLimit(t *testing.T) {
	c := &PGExtensionCollector{
		maxDatabases:     3,
		includeDatabases: []string{},
	}

	eligible := []string{"db1", "db2", "db3", "db4", "db5"}
	result := c.selectDatabases(eligible)

	if len(result) != 3 {
		t.Errorf("expected 3 databases, got %d", len(result))
	}
}

func TestPGExtensionCollector_selectDatabases_PriorityFirst(t *testing.T) {
	c := &PGExtensionCollector{
		maxDatabases:     3,
		includeDatabases: []string{"priority_db"},
	}

	eligible := []string{"db1", "db2", "priority_db", "db3", "db4"}
	result := c.selectDatabases(eligible)

	if len(result) != 3 {
		t.Errorf("expected 3 databases, got %d", len(result))
	}

	// Priority database should always be first
	if result[0] != "priority_db" {
		t.Errorf("expected priority_db to be first, got %q", result[0])
	}
}

func TestPGExtensionCollector_selectDatabases_MultiplePriority(t *testing.T) {
	c := &PGExtensionCollector{
		maxDatabases:     4,
		includeDatabases: []string{"priority1", "priority2"},
	}

	eligible := []string{"db1", "priority1", "db2", "priority2", "db3"}
	result := c.selectDatabases(eligible)

	if len(result) != 4 {
		t.Errorf("expected 4 databases, got %d", len(result))
	}

	// Both priority databases should be first, in order
	if result[0] != "priority1" {
		t.Errorf("expected priority1 to be first, got %q", result[0])
	}
	if result[1] != "priority2" {
		t.Errorf("expected priority2 to be second, got %q", result[1])
	}
}

func TestPGExtensionCollector_selectDatabases_PriorityNotInEligible(t *testing.T) {
	c := &PGExtensionCollector{
		maxDatabases:     3,
		includeDatabases: []string{"nonexistent_db", "existing_priority"},
	}

	eligible := []string{"db1", "db2", "existing_priority", "db3"}
	result := c.selectDatabases(eligible)

	if len(result) != 3 {
		t.Errorf("expected 3 databases, got %d", len(result))
	}

	// Only the existing priority should be included
	if result[0] != "existing_priority" {
		t.Errorf("expected existing_priority to be first, got %q", result[0])
	}
}

func TestPGExtensionCollector_selectDatabases_LimitLessThanEligible(t *testing.T) {
	c := &PGExtensionCollector{
		maxDatabases:     2,
		includeDatabases: []string{},
	}

	eligible := []string{"db1", "db2", "db3"}
	result := c.selectDatabases(eligible)

	if len(result) != 2 {
		t.Errorf("expected 2 databases, got %d", len(result))
	}
}

func TestPGExtensionCollector_selectDatabases_LimitGreaterThanEligible(t *testing.T) {
	c := &PGExtensionCollector{
		maxDatabases:     100,
		includeDatabases: []string{},
	}

	eligible := []string{"db1", "db2", "db3"}
	result := c.selectDatabases(eligible)

	if len(result) != 3 {
		t.Errorf("expected 3 databases (all eligible), got %d", len(result))
	}
}
