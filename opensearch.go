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

// MustNewZapLoggerWithOpenSearch creates a zap logger with OpenSearch support
func MustNewZapLoggerWithOpenSearch(opts ...LogOptFunc) (*zap.Logger, func() error) {
	opt := &LogOpts{
		level:       zapcore.InfoLevel,
		withConsole: false,
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
		core, writer, err := newOpenSearchCore(opt.openSearchConfig, opt.openSearchIndex, opt.level, opt.internalLogger)
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
	indexName     string
	indexerConfig opensearchutil.BulkIndexerConfig
	mu            sync.Mutex
	closed        bool
	logger        *zap.Logger
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

func newOpenSearchCore(config *opensearch.Config, index string, level zapcore.Level, logger *zap.Logger) (zapcore.Core, *openSearchWriter, error) {
	client, err := opensearch.NewClient(*config)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create OpenSearch client: %v", err)
	}

	indexerConfig := opensearchutil.BulkIndexerConfig{
		Client:        client,
		Index:         index,
		NumWorkers:    2,
		FlushBytes:    256 * 1024, // 1MB
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
		indexName:     index,
		indexerConfig: indexerConfig,
		logger:        logger,
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
