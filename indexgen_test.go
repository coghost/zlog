package zlog

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDailyIndexNameGenerator(t *testing.T) {
	tests := []struct {
		name     string
		config   IndexConfig
		timeFunc func() time.Time
		want     string
	}{
		{
			name: "UTC with dot format",
			config: IndexConfig{
				BaseIndexName: "logs",
				Format:        string(DateFormatDot),
				Location:      time.UTC,
			},
			timeFunc: func() time.Time {
				return time.Date(2024, 1, 25, 12, 0, 0, 0, time.UTC)
			},
			want: "logs-2024.01.25",
		},
		{
			name: "Tokyo timezone with dash format",
			config: IndexConfig{
				BaseIndexName: "app-logs",
				Format:        string(DateFormatDash),
				Location:      MustLoadLocation("Asia/Tokyo"),
			},
			timeFunc: func() time.Time {
				// This is 2024-01-25 00:00:00 UTC, which should be
				// 2024-01-25 09:00:00 in Tokyo
				return time.Date(2024, 1, 25, 0, 0, 0, 0, time.UTC)
			},
			want: "app-logs-2024-01-25",
		},
		{
			name: "Short format",
			config: IndexConfig{
				BaseIndexName: "metrics",
				Format:        string(DateFormatShort),
				Location:      time.UTC,
			},
			timeFunc: func() time.Time {
				return time.Date(2024, 1, 25, 12, 0, 0, 0, time.UTC)
			},
			want: "metrics-20240125",
		},
		{
			name: "Custom hourly format",
			config: IndexConfig{
				BaseIndexName: "hourly-logs",
				Format:        "2006-01-02-15",
				Location:      time.UTC,
			},
			timeFunc: func() time.Time {
				return time.Date(2024, 1, 25, 14, 30, 0, 0, time.UTC)
			},
			want: "hourly-logs-2024-01-25-14",
		},
		{
			name: "Default format when empty",
			config: IndexConfig{
				BaseIndexName: "default-logs",
				Format:        "",
				Location:      time.UTC,
			},
			timeFunc: func() time.Time {
				return time.Date(2024, 1, 25, 12, 0, 0, 0, time.UTC)
			},
			want: "default-logs-2024.01.25",
		},
		{
			name: "Default UTC when location is nil",
			config: IndexConfig{
				BaseIndexName: "utc-logs",
				Format:        string(DateFormatDot),
				Location:      nil,
			},
			timeFunc: func() time.Time {
				return time.Date(2024, 1, 25, 12, 0, 0, 0, time.UTC)
			},
			want: "utc-logs-2024.01.25",
		},
	}

	// Store original time.Now
	originalTimeNow := timeNow
	defer func() { timeNow = originalTimeNow }()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Mock time.Now for this test
			timeNow = tt.timeFunc

			generator := NewIndexGenerator(tt.config)
			got := generator.GetIndexName()
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestIndexRotation(t *testing.T) {
	// Store original time.Now
	originalTimeNow := timeNow
	defer func() { timeNow = originalTimeNow }()

	// Create a generator with UTC timezone
	generator := NewIndexGenerator(IndexConfig{
		BaseIndexName: "app-logs",
		Format:        string(DateFormatDot),
		Location:      time.UTC,
	})

	// Test scenarios for rotation
	scenarios := []struct {
		timestamp time.Time
		expected  string
	}{
		{
			// Day 1
			timestamp: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			expected:  "app-logs-2024.01.01",
		},
		{
			// Same day, different hour
			timestamp: time.Date(2024, 1, 1, 23, 59, 59, 0, time.UTC),
			expected:  "app-logs-2024.01.01",
		},
		{
			// Next day
			timestamp: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
			expected:  "app-logs-2024.01.02",
		},
		{
			// Month transition
			timestamp: time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC),
			expected:  "app-logs-2024.02.01",
		},
		{
			// Year transition
			timestamp: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			expected:  "app-logs-2025.01.01",
		},
	}

	for _, sc := range scenarios {
		timeNow = func() time.Time { return sc.timestamp }
		got := generator.GetIndexName()
		assert.Equal(t, sc.expected, got, "Time: "+sc.timestamp.String())
	}
}

func TestHourlyRotation(t *testing.T) {
	// Store original time.Now
	originalTimeNow := timeNow
	defer func() { timeNow = originalTimeNow }()

	// Create a generator with hourly rotation
	generator := NewIndexGenerator(IndexConfig{
		BaseIndexName: "hourly-logs",
		Format:        "2006.01.02-15",
		Location:      time.UTC,
	})

	scenarios := []struct {
		timestamp time.Time
		expected  string
	}{
		{
			timestamp: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			expected:  "hourly-logs-2024.01.01-00",
		},
		{
			// Same hour
			timestamp: time.Date(2024, 1, 1, 0, 59, 59, 0, time.UTC),
			expected:  "hourly-logs-2024.01.01-00",
		},
		{
			// Next hour
			timestamp: time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
			expected:  "hourly-logs-2024.01.01-01",
		},
		{
			// Day transition
			timestamp: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
			expected:  "hourly-logs-2024.01.02-00",
		},
	}

	for _, sc := range scenarios {
		timeNow = func() time.Time { return sc.timestamp }
		got := generator.GetIndexName()
		assert.Equal(t, sc.expected, got, "Time: "+sc.timestamp.String())
	}
}

func TestTimezoneRotation(t *testing.T) {
	// Store original time.Now
	originalTimeNow := timeNow
	defer func() { timeNow = originalTimeNow }()

	// Create generators for different timezones
	utcGen := NewIndexGenerator(IndexConfig{
		BaseIndexName: "logs",
		Format:        string(DateFormatDot),
		Location:      time.UTC,
	})

	tokyoGen := NewIndexGenerator(IndexConfig{
		BaseIndexName: "logs",
		Format:        string(DateFormatDot),
		Location:      MustLoadLocation("Asia/Tokyo"),
	})

	// Test UTC midnight and corresponding Tokyo time
	scenarios := []struct {
		timestamp   time.Time
		expectedUTC string
		expectedJST string
		description string
	}{
		{
			// When it's UTC midnight (00:00), it's 09:00 JST
			timestamp:   time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			expectedUTC: "logs-2024.01.01",
			expectedJST: "logs-2024.01.01",
			description: "UTC midnight",
		},
		{
			// When it's 15:00 UTC, it's 00:00 JST next day
			timestamp:   time.Date(2024, 1, 1, 15, 0, 0, 0, time.UTC),
			expectedUTC: "logs-2024.01.01",
			expectedJST: "logs-2024.01.02",
			description: "JST midnight",
		},
	}

	for _, sc := range scenarios {
		t.Run(sc.description, func(t *testing.T) {
			timeNow = func() time.Time { return sc.timestamp }

			gotUTC := utcGen.GetIndexName()
			assert.Equal(t, sc.expectedUTC, gotUTC, "UTC index at "+sc.timestamp.String())

			gotJST := tokyoGen.GetIndexName()
			assert.Equal(t, sc.expectedJST, gotJST, "JST index at "+sc.timestamp.String())
		})
	}
}
