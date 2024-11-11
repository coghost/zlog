package zlog

import (
	"fmt"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestMustNewZapLoggerWithOpenSearch(t *testing.T) {
	opensearchURL := "https://localhost:9200"
	insecure := true // Set to false if you have valid certificates

	// Check if OpenSearch is ready
	if !IsOpenSearchReady(opensearchURL, 5*time.Second, insecure) {
		msg := "OpenSearch is not ready. Skipping test."
		fmt.Println(msg)
		t.Skip(msg)
	}

	// Create a logger with OpenSearch enabled
	defaultConfig := DefaultOpenSearchConfig(opensearchURL, insecure)
	logger, flushFunc := MustNewZapLoggerWithOpenSearch(
		WithOpenSearchConfig(&defaultConfig),
		WithOpenSearchIndex("zlog-test"),
		WithLogLevel(zapcore.InfoLevel),
		WithConsole(true),
	)

	defer FlushLogsWithTimeout(flushFunc, 30*time.Second, logger)()

	// Log some test messages with timestamps
	now := time.Now()
	logger.Info("Test info message v7", zap.Time("timestamp", now))
	logger.Warn("Test warning message v7", zap.Time("timestamp", now.Add(time.Second)))
	logger.Error("Test error message v7", zap.Time("timestamp", now.Add(2*time.Second)))

	// Add some structured fields with a timestamp
	logger.Info("Structured log message v7",
		zap.Time("timestamp", now.Add(3*time.Second)),
		zap.String("component", "test"),
		zap.Int("attempt", 3),
		zap.Duration("backoff", time.Second*5),
	)

	// Log messages with different timestamps
	for i := 0; i < 5; i++ {
		logger.Info("Repeated log message v7",
			zap.Time("timestamp", now.Add(time.Duration(4+i)*time.Second)),
			zap.Int("iteration", i+1),
		)

		time.Sleep(100 * time.Millisecond) // Small delay between logs
	}

	t.Log("Logs sent to OpenSearch. Please verify in the OpenSearch dashboard.")
}

func TestLogToOpenSearch(t *testing.T) {
	opensearchURL := "https://localhost:9200"
	insecure := true // Set to false if you have valid certificates

	// Check if OpenSearch is ready
	if !IsOpenSearchReady(opensearchURL, 5*time.Second, insecure) {
		msg := "OpenSearch is not ready. Skipping test."
		fmt.Println(msg)
		t.Skip(msg)
	}

	defaultConfig := DefaultOpenSearchConfig(opensearchURL, insecure)
	logger, flushFunc := MustNewZapLoggerWithOpenSearch(
		WithOpenSearchConfig(&defaultConfig),
		WithOpenSearchIndex("zlog-test"),
		WithLogLevel(zapcore.InfoLevel),
		WithConsole(true),
	)

	// Log some test messages with timestamps and flush after each
	now := time.Now()
	logger.Info("Test info message v4", zap.Time("timestamp", now))
	if err := flushFunc(); err != nil {
		t.Errorf("Failed to flush logs: %v", err)
	}

	logger.Warn("Test warning message v4", zap.Time("timestamp", now.Add(time.Second)))
	if err := flushFunc(); err != nil {
		t.Errorf("Failed to flush logs: %v", err)
	}

	logger.Error("Test error message v4", zap.Time("timestamp", now.Add(2*time.Second)))
	if err := flushFunc(); err != nil {
		t.Errorf("Failed to flush logs: %v", err)
	}

	// Add some structured fields with a timestamp
	logger.Info("Structured log message v4",
		zap.Time("timestamp", now.Add(3*time.Second)),
		zap.String("component", "test"),
		zap.Int("attempt", 3),
		zap.Duration("backoff", time.Second*5),
	)
	if err := flushFunc(); err != nil {
		t.Errorf("Failed to flush logs: %v", err)
	}

	// Log messages with different timestamps
	for i := 0; i < 5; i++ {
		logger.Info("Repeated log message v4",
			zap.Time("timestamp", now.Add(time.Duration(4+i)*time.Second)),
			zap.Int("iteration", i+1),
		)
		if err := flushFunc(); err != nil {
			t.Errorf("Failed to flush logs: %v", err)
		}
		time.Sleep(100 * time.Millisecond) // Small delay between logs
	}

	t.Log("Logs sent to OpenSearch. Please verify in the OpenSearch dashboard.")
}
