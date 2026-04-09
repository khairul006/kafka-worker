package logger

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

var Log *zap.Logger

func InitLogger() {
	// 1. Setup Lumberjack for Rotation
	writerSync := zapcore.AddSync(&lumberjack.Logger{
		Filename:   "./logs/kafka-worker.log",
		MaxSize:    100,  // Megabytes before rotating
		MaxBackups: 5,    // Keep last 5 old log files
		MaxAge:     30,   // Keep logs for 30 days
		Compress:   true, // Zip old log files
	})

	// 2. Setup Encoder (JSON is better for production/ELK)
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	encoder := zapcore.NewJSONEncoder(encoderConfig)

	// 3. Create Core
	// We combine File output and Standard Output (Console)
	core := zapcore.NewTee(
		zapcore.NewCore(encoder, writerSync, zap.InfoLevel),
		zapcore.NewCore(zapcore.NewConsoleEncoder(encoderConfig), zapcore.AddSync(os.Stdout), zap.InfoLevel),
	)

	Log = zap.New(core, zap.AddCaller())
}
