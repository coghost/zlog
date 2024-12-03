package zlog

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/opensearch-project/opensearch-go"
	"github.com/opensearch-project/opensearch-go/opensearchutil"
)

func DefaultOpenSearchConfig(url string, insecure bool) opensearch.Config {
	return opensearch.Config{
		Addresses: []string{url},
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: insecure},
		},
	}
}

// MustNewZapLoggerWithOpenSearch creates a zap logger with OpenSearch support.
// It panics if the required OpenSearch configuration is missing or if initialization fails.
//
// Parameters:
//   - opts: Variadic LogOptFunc parameters for configuring the logger
//
// Returns:
//   - *zap.Logger: A configured zap logger instance
//   - func() error: A flush function that should be called before program termination
//     to ensure all logs are written to OpenSearch
//
// OpenSearch Configuration:
//   - Bulk Indexing: Uses OpenSearch bulk API for efficient log shipping
//   - Workers: 2 concurrent workers for processing logs
//   - Buffer Size: 256KB before forcing flush
//   - Flush Interval: Every 10 seconds
//   - Error Handling: Logs bulk indexing errors through internal logger
//
// The function supports both console and OpenSearch output. When OpenSearch is enabled,
// both openSearchConfig and openSearchIndex must be provided through the options.
func MustNewZapLoggerWithOpenSearch(opts ...LogOptFunc) (*zap.Logger, func() error) {
	opt := &LogOpts{
		level:       zapcore.InfoLevel,
		withConsole: false,
		// rotate log configs
		indexDateFormat: string(DateFormatDot), // Default format
		timeLocation:    time.UTC,              // Default timezone
	}
	bindLogOpts(opt, opts...)

	// If no internal logger is provided, create a no-op logger
	if opt.internalLogger == nil {
		opt.internalLogger = zap.NewNop()
	}

	var cores []zapcore.Core

	if opt.withConsole {
		consoleEnc := genProdEncoder()
		coreConsole := zapcore.NewCore(consoleEnc, zapcore.AddSync(os.Stdout), opt.level)
		cores = append(cores, coreConsole)
	}

	if opt.openSearchConfig == nil {
		opt.internalLogger.Panic("OpenSearch config must be provided when OpenSearch logging is enabled")
	}

	if opt.openSearchIndex == "" {
		opt.internalLogger.Panic("OpenSearch index must be provided when OpenSearch logging is enabled")
	}

	var openSearchWriter *openSearchWriter

	createOpenSearchCore := func() (zapcore.Core, error) {
		indexNameGenerator := NewIndexGenerator(IndexConfig{
			BaseIndexName: opt.openSearchIndex,
			Format:        opt.indexDateFormat,
			Location:      opt.timeLocation,
		})

		core, writer, err := newOpenSearchCore(
			opt.openSearchConfig,
			indexNameGenerator,
			opt.level,
			opt.internalLogger,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create OpenSearch core: %v", err)
		}
		openSearchWriter = writer
		return core, nil
	}

	openSearchCore, err := createOpenSearchCore()
	if err != nil {
		opt.internalLogger.Panic("Failed to create OpenSearch core", zap.Error(err))
	}

	cores = append(cores, openSearchCore)

	if len(cores) == 0 {
		opt.internalLogger.Panic("No logging outputs specified")
	}

	coreTee := zapcore.NewTee(cores...)
	logger := zap.New(coreTee, zap.AddCaller())

	flushFunc := func() error {
		if openSearchWriter != nil {
			err := openSearchWriter.Flush()
			if err != nil {
				return err
			}
			newCore, err := createOpenSearchCore()
			if err != nil {
				return err
			}
			for i, core := range cores {
				if core == openSearchCore {
					cores[i] = newCore
					openSearchCore = newCore
					break
				}
			}
		}
		return nil
	}

	return logger, flushFunc
}

// FlushLogsWithTimeout attempts to flush logs with a timeout.
// It returns a function suitable for use with defer.
func FlushLogsWithTimeout(flushFunc func() error, timeout time.Duration, logger *zap.Logger) func() {
	return func() {
		flushDone := make(chan error, 1)
		go func() {
			flushDone <- flushFunc()
		}()

		select {
		case err := <-flushDone:
			if err != nil {
				logger.Error("Error during flush", zap.Error(err))
			} else {
				logger.Info("Logs flushed successfully")
			}
		case <-time.After(timeout):
			logger.Error("Flush operation timed out", zap.Duration("timeout", timeout))
		}
	}
}

type openSearchWriter struct {
	indexer       opensearchutil.BulkIndexer
	client        *opensearch.Client
	indexerConfig opensearchutil.BulkIndexerConfig
	mu            sync.Mutex
	closed        bool
	logger        *zap.Logger

	indexNameGenerator *IndexGenerator
}

func (w *openSearchWriter) Write(p []byte) (n int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	var logEntry map[string]interface{}
	err = json.Unmarshal(p, &logEntry)
	if err != nil {
		return 0, fmt.Errorf("failed to parse log entry: %w", err)
	}

	encodedEntry, err := json.Marshal(logEntry)
	if err != nil {
		return 0, fmt.Errorf("failed to re-encode log entry: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = w.indexer.Add(
		ctx,
		opensearchutil.BulkIndexerItem{
			Action: "index",
			Index:  w.indexNameGenerator.GetIndexName(), // Use dynamic index name
			Body:   bytes.NewReader(encodedEntry),
		},
	)
	if err != nil {
		return 0, fmt.Errorf("failed to add document to bulk indexer: %w", err)
	}

	return len(p), nil
}

func (w *openSearchWriter) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	stats := w.indexer.Stats()
	w.logger.Info("Flushing logs",
		zap.Uint64("added", stats.NumAdded),
		zap.Uint64("flushed", stats.NumFlushed),
		zap.Uint64("failed", stats.NumFailed))

	err := w.indexer.Close(ctx)
	if err != nil {
		w.logger.Error("Error closing bulk indexer", zap.Error(err))
		return fmt.Errorf("error closing bulk indexer: %w", err)
	}

	stats = w.indexer.Stats()
	w.logger.Info("Flush completed",
		zap.Uint64("added", stats.NumAdded),
		zap.Uint64("flushed", stats.NumFlushed),
		zap.Uint64("failed", stats.NumFailed))

	return nil
}

// newOpenSearchCore creates a new zapcore.Core that writes logs to OpenSearch.
//
// Parameters:
//   - config: OpenSearch client configuration (*opensearch.Config)
//   - index: Name of the OpenSearch index to write logs to
//   - level: Minimum log level to process (zapcore.Level)
//   - logger: Internal logger for reporting indexing errors
//
// Returns:
//   - zapcore.Core: The configured logging core
//   - *openSearchWriter: The underlying OpenSearch writer
//   - error: Any error that occurred during setup
//
// BulkIndexer Configuration:
//   - NumWorkers: 2 concurrent workers for processing log entries
//   - FlushBytes: 256KB buffer size before forcing flush
//   - FlushInterval: 10 seconds interval for automatic flushing
//
// The function initializes a bulk indexer for efficient log shipping to OpenSearch
// and configures JSON encoding for the log entries. It uses worker pools and
// buffering for optimized performance.
func newOpenSearchCore(config *opensearch.Config, indexNameGenerator *IndexGenerator, level zapcore.Level, logger *zap.Logger) (zapcore.Core, *openSearchWriter, error) {
	client, err := opensearch.NewClient(*config)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create OpenSearch client: %v", err)
	}

	indexerConfig := opensearchutil.BulkIndexerConfig{
		Client:        client,
		Index:         indexNameGenerator.GetIndexName(),
		NumWorkers:    2,
		FlushBytes:    256 * 1024,
		FlushInterval: 10 * time.Second,
		OnError: func(ctx context.Context, err error) {
			logger.Error("Bulk indexer error", zap.Error(err))
		},
	}

	indexer, err := opensearchutil.NewBulkIndexer(indexerConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create bulk indexer: %v", err)
	}

	writer := &openSearchWriter{
		indexer:       indexer,
		client:        client,
		indexerConfig: indexerConfig,
		logger:        logger,
		// dynamically generate index name
		indexNameGenerator: indexNameGenerator,
	}

	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	openSearchEncoder := zapcore.NewJSONEncoder(encoderConfig)

	return zapcore.NewCore(
		openSearchEncoder,
		zapcore.AddSync(writer),
		level,
	), writer, nil
}

func IsOpenSearchReady(url string, timeout time.Duration, insecure bool) bool {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return false
	}

	transport := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: insecure},
	}
	client := &http.Client{Transport: transport}

	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	return resp.StatusCode == http.StatusOK
}
