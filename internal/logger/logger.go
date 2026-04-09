package logger

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

var Log *zap.Logger

func InitLogger() {
	writerSync := zapcore.AddSync(&lumberjack.Logger{
		Filename:   "./logs/kafka-worker.log",
		MaxSize:    100,  // Megabytes before rotating
		MaxBackups: 5,    // Keep last 5 old log files
		MaxAge:     30,   // Keep logs for 30 days
		Compress:   true, // Zip old log files
	})

	// 1. Define the actual config we will use
	config := zapcore.EncoderConfig{
		TimeKey:          "ts",
		LevelKey:         "level",
		MessageKey:       "msg",
		LineEnding:       zapcore.DefaultLineEnding,
		EncodeTime:       zapcore.TimeEncoderOfLayout("2006-01-02 15:04:05.000"),
		EncodeLevel:      zapcore.CapitalLevelEncoder,
		ConsoleSeparator: " - ",
	}

	// 2. Create the encoder using that config
	encoder := zapcore.NewConsoleEncoder(config)

	// 3. Setup the Core
	core := zapcore.NewTee(
		zapcore.NewCore(encoder, writerSync, zap.InfoLevel),
		zapcore.NewCore(encoder, zapcore.AddSync(os.Stdout), zap.InfoLevel),
	)

	// Finalize the logger (No zap.AddCaller() to keep it short)
	Log = zap.New(core)
}

func Info(message string, data ...interface{}) {
	var additionalData interface{}
	if len(data) > 0 {
		additionalData = data[0]
	}
	Log.Info("Message: "+message+" - Additional Data:", zap.Any("data", additionalData))
}

func Debug(message string, data ...interface{}) {
	var additionalData interface{}
	if len(data) > 0 {
		additionalData = data[0]
	}
	Log.Debug("Message: "+message+" - Additional Data:", zap.Any("data", additionalData))
}

func Warn(message string, data ...interface{}) {
	var additionalData interface{}
	if len(data) > 0 {
		additionalData = data[0]
	}
	Log.Warn("Message: "+message+" - Additional Data:", zap.Any("data", additionalData))
}

// Error wrapper: Message + Data + Automatic Stack Trace
func Error(message string, data ...interface{}) {
	var d interface{}
	if len(data) > 0 {
		d = data[0]
	}

	// zap.AddStacktrace(zap.ErrorLevel) ensures a trace is attached
	// only for Error level and above.
	Log.Error("Message: "+message+" - Additional Data:",
		zap.Any("data", d),
		zap.Stack("trace"),
	)
}

// Fatal wrapper: Message + Data + Trace + App Crash
func Fatal(message string, data ...interface{}) {
	var d interface{}
	if len(data) > 0 {
		d = data[0]
	}

	Log.Fatal("Message: "+message+" - Additional Data:",
		zap.Any("data", d),
		zap.Stack("trace"),
	)
}
