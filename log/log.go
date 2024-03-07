package log

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

type Logger interface {
	Error(v ...interface{})
	Warn(v ...interface{})
	Info(v ...interface{})
	Debug(v ...interface{})
	Errorf(format string, v ...interface{})
	Warnf(format string, v ...interface{})
	Infof(format string, v ...interface{})
	Debugf(format string, v ...interface{})
}

var (
	defaultLogger Logger
)

func init() {
	defaultLogger = newSugarLogger(NewOptions())
}

// Options 选项配置
type Options struct {
	LogName    string // 日志名称
	LogLevel   string // 日志级别
	FileName   string // 文件名称
	MaxAge     int    // 日志保留时间，以天为单位
	MaxSize    int    // 日志保留大小，以 M 为单位
	MaxBackups int    // 保留文件个数
	Compress   bool   // 是否压缩
}

// Option 选项方法
type Option func(*Options)

// NewOptions 初始化
func NewOptions(opts ...Option) Options {

	options := Options{
		LogName:    "app",
		LogLevel:   "info",
		FileName:   "app.log",
		MaxAge:     10,
		MaxSize:    100,
		MaxBackups: 3,
		Compress:   true,
	}
	for _, opt := range opts {
		opt(&options)
	}
	return options
}

func WithMaxAge(day int) Option {
	return func(options *Options) {
		options.MaxAge = day
	}
}

func WithMaxSize(size int) Option {
	return func(options *Options) {
		options.MaxSize = size
	}
}

func WithMaxBackups(cnt int) Option {
	return func(options *Options) {
		options.MaxBackups = cnt
	}
}

func WithCompress(flag bool) Option {
	return func(options *Options) {
		options.Compress = flag
	}
}

func WithLogName(name string) Option {
	return func(options *Options) {
		options.LogName = name
	}
}

// WithLogLevel 日志级别
func WithLogLevel(level string) Option {
	return func(o *Options) {
		o.LogLevel = level
	}
}

// WithFileName 日志文件
func WithFileName(filename string) Option {
	return func(o *Options) {
		o.FileName = filename
	}
}

// Levels zapcore level
var Levels = map[string]zapcore.Level{
	"":      zapcore.DebugLevel,
	"debug": zapcore.DebugLevel,
	"info":  zapcore.InfoLevel,
	"warn":  zapcore.WarnLevel,
	"error": zapcore.ErrorLevel,
	"fatal": zapcore.FatalLevel,
}

type zapLoggerWrapper struct {
	*zap.SugaredLogger
	options Options
}

func (w *zapLoggerWrapper) getEncoder() zapcore.Encoder {
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	// 在日志文件中使用大写字母记录日志级别
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	return zapcore.NewConsoleEncoder(encoderConfig)

}

func (w *zapLoggerWrapper) getLogWriter() zapcore.WriteSyncer {
	return zapcore.AddSync(&lumberjack.Logger{
		Filename:   w.options.FileName,
		MaxAge:     w.options.MaxAge,
		MaxSize:    w.options.MaxSize,
		MaxBackups: w.options.MaxBackups,
		Compress:   w.options.Compress,
	})
}

func newSugarLogger(options Options) *zapLoggerWrapper {
	w := &zapLoggerWrapper{options: options}
	encoder := w.getEncoder()
	writeSyncer := w.getLogWriter()
	core := zapcore.NewCore(encoder, writeSyncer, Levels[options.LogLevel])
	w.SugaredLogger = zap.New(core, zap.AddCaller(), zap.AddCallerSkip(1)).Sugar()
	return w
}

func GetDefaultLogger() Logger {
	return defaultLogger
}
