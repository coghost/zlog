package zlog

import (
	"fmt"
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

type LogOpts struct {
	devEnv      bool
	withLJ      bool
	withConsole bool

	level zapcore.Level

	ljFilename   string
	lumberJacker *lumberjack.Logger
}

type LogOptFunc func(o *LogOpts)

func bindLogOpts(opt *LogOpts, opts ...LogOptFunc) {
	for _, f := range opts {
		f(opt)
	}
}

func WithDevEnv(b bool) LogOptFunc {
	return func(o *LogOpts) {
		o.devEnv = b
	}
}

func WithLJ(b bool) LogOptFunc {
	return func(o *LogOpts) {
		o.withLJ = b
	}
}

func WithConsole(b bool) LogOptFunc {
	return func(o *LogOpts) {
		o.withConsole = b
	}
}

func WithLogLevel(lvl zapcore.Level) LogOptFunc {
	return func(o *LogOpts) {
		o.level = lvl
	}
}

// WithLjFilename if name is supplied
func WithLjFilename(s string) LogOptFunc {
	return func(o *LogOpts) {
		o.ljFilename = s
	}
}

// MustNewZapLogger create a simple zap logger
func MustNewZapLogger(opts ...LogOptFunc) *zap.Logger {
	opt := &LogOpts{devEnv: true, level: zapcore.InfoLevel, withLJ: true, withConsole: true}
	bindLogOpts(opt, opts...)

	if opt.lumberJacker == nil {
		filename := "/tmp/zlog.log"
		if opt.ljFilename != "" {
			filename = opt.ljFilename
		}

		opt.lumberJacker = newLJ(filename)
	}

	lumberJackEnc := genProdEncoder()
	consoleEnc := lumberJackEnc

	if opt.devEnv {
		lumberJackEnc = genDevEncoder(false)
		consoleEnc = genDevEncoder(true)
	}

	writeSyncer := zapcore.AddSync(opt.lumberJacker)
	coreLumerJack := zapcore.NewCore(lumberJackEnc, writeSyncer, opt.level)
	coreConsole := zapcore.NewCore(consoleEnc, zapcore.AddSync(os.Stdout), opt.level)

	var cores []zapcore.Core
	if opt.withLJ {
		cores = append(cores, coreLumerJack)
	}

	if opt.withConsole {
		cores = append(cores, coreConsole)
	}

	if len(cores) == 0 {
		fmt.Println("either lumberJack or console is required, but you disabled all")
		return nil
	}

	coreTee := zapcore.NewTee(cores...)
	logger := zap.New(coreTee, zap.AddCaller())

	if opt.devEnv {
		ReplaceGlobalToShowLogZapL(logger)
	}

	return logger
}

func genProdEncoder() zapcore.Encoder { //nolint
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder

	return zapcore.NewConsoleEncoder(encoderConfig)
}

func genDevEncoder(isConsole bool) zapcore.Encoder { //nolint
	encoderConfig := zap.NewDevelopmentEncoderConfig()
	encoderConfig.EncodeTime = zapcore.TimeEncoderOfLayout("15:04:05")
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder

	if isConsole {
		encoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
		encoderConfig.ConsoleSeparator = " "
	}

	return zapcore.NewConsoleEncoder(encoderConfig)
}

func ReplaceGlobalToShowLogZapL(logger *zap.Logger) {
	// zap.L().Debug("global zap logger is replaced.")
	zap.ReplaceGlobals(logger)
}

func newLJ(filename string) *lumberjack.Logger {
	const (
		backupFiles = 5
		days        = 30
		size        = 10
	)

	lumberJackLogger := &lumberjack.Logger{
		Filename:   filename,
		MaxSize:    size,
		MaxBackups: backupFiles,
		MaxAge:     days,
		Compress:   false,
	}

	return lumberJackLogger
}
