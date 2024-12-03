package zlog

import (
	"fmt"
	"time"
)

// IndexFormat defines standard time formats for index naming
type IndexFormat string

// Predefined index formats
const (
	DateFormatDot   IndexFormat = "2006.01.02" // example: logs-2024.01.25
	DateFormatDash  IndexFormat = "2006-01-02" // example: logs-2024-01-25
	DateFormatShort IndexFormat = "20060102"   // example: logs-20240125
)

// For testing purposes
var timeNow = time.Now

// IndexGenerator generates time-based index names
type IndexGenerator struct {
	baseIndexName string
	format        string
	location      *time.Location
}

// IndexConfig configures how index names are generated
type IndexConfig struct {
	BaseIndexName string
	Format        string         // If empty, defaults to FormatDot
	Location      *time.Location // If nil, defaults to UTC
}

// NewIndexGenerator creates a new index name generator
func NewIndexGenerator(config IndexConfig) *IndexGenerator {
	if config.Location == nil {
		config.Location = time.UTC
	}

	if config.Format == "" {
		config.Format = string(DateFormatDot) // Default format "2006.01.02"
	}

	return &IndexGenerator{
		baseIndexName: config.BaseIndexName,
		format:        config.Format,
		location:      config.Location,
	}
}

func (g *IndexGenerator) GetIndexName() string {
	return fmt.Sprintf("%s-%s", g.baseIndexName, timeNow().In(g.location).Format(g.format))
}

// LoadLocation loads a timezone location or panics on error
func MustLoadLocation(name string) *time.Location {
	loc, err := time.LoadLocation(name)
	if err != nil {
		panic(err)
	}

	return loc
}
