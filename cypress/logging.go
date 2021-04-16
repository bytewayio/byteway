package cypress

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

// LogLevel logging level
type LogLevel int32

const (
	// LogLevelDebug debug logging level
	LogLevelDebug LogLevel = 1 + iota

	// LogLevelInfo info logging level
	LogLevelInfo

	// LogLevelWarn warnning level
	LogLevelWarn

	// LogLevelError error level
	LogLevelError
)

var (
	// CorrelationIDHeader http header name for correlation id header
	CorrelationIDHeader = http.CanonicalHeaderKey("x-correlation-id")
)

type traceableResponseWriter struct {
	statusCode    int
	contentLength int
	writer        http.ResponseWriter
}

func (w *traceableResponseWriter) Header() http.Header {
	return w.writer.Header()
}

func (w *traceableResponseWriter) Write(data []byte) (int, error) {
	if data != nil {
		w.contentLength += len(data)
	}

	return w.writer.Write(data)
}

func (w *traceableResponseWriter) WriteHeader(statusCode int) {
	w.statusCode = statusCode
	w.writer.WriteHeader(statusCode)
}

func (w *traceableResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := w.writer.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("the ResponseWriter doesn't support the Hijacker interface")
	}

	return hijacker.Hijack()
}

// NewRollingLogWriter returns a new file based rolling log writer
// maxSizeInMegaBytes specifies the maximum size of each log file, while maxRotationFiles
// tells the maximum files to keep
func NewRollingLogWriter(fileName string, maxSizeInMegaBytes, maxRotationFiles int) io.Writer {
	return &lumberjack.Logger{
		Filename:   fileName,
		MaxSize:    maxSizeInMegaBytes,
		MaxBackups: maxRotationFiles,
		Compress:   false,
	}
}

// SetupLogger setup the global logger with specified log writer and log level
// this must be called before the logging can actually work
func SetupLogger(level LogLevel, writer io.Writer) {
	logLevel := zap.DebugLevel
	switch level {
	case LogLevelInfo:
		logLevel = zap.InfoLevel
	case LogLevelWarn:
		logLevel = zap.WarnLevel
	case LogLevelError:
		logLevel = zap.ErrorLevel
	}

	jsonEncoder := zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig())
	writeSyncer := zapcore.AddSync(writer)
	zapCore := zapcore.NewCore(jsonEncoder, writeSyncer, logLevel)
	logger := zap.New(zapCore)
	zap.ReplaceGlobals(logger)
}

// GetTraceID get the trace ID related to the context
func GetTraceID(ctx context.Context) string {
	value := ctx.Value(TraceActivityIDKey)
	if value != nil {
		if traceID, ok := value.(string); ok {
			return traceID
		}
	}

	return ""
}

// LoggingHandler http incoming logging handler
func LoggingHandler(handler http.Handler) http.Handler {
	handlerFunction := func(writer http.ResponseWriter, request *http.Request) {
		// log panic error
		defer func() {
			if err := recover(); err != nil {
				defer zap.L().Sync()
				// Log and continue, the user code has to ensure all locks will be unlocked
				// in case of panic
				zap.L().Error(fmt.Sprint(err),
					zap.String("requestUri", request.URL.String()),
					zap.String("path", request.URL.Path),
					zap.String("requestMethod", request.Method),
					zap.Stack("source"),
					zap.String("activityId", GetTraceID(request.Context())))
				SendError(writer, 500, "unexpected server error")
			}
		}()

		var correlationID string
		activityID := uuid.NewString()
		timeNow := time.Now()
		headerValues, ok := request.Header[CorrelationIDHeader]
		if ok && len(headerValues) > 0 {
			correlationID = headerValues[0]
		} else {
			correlationID = uuid.NewString()
		}

		tw := &traceableResponseWriter{
			statusCode:    200,
			contentLength: 0,
			writer:        writer,
		}
		newRequest := request.WithContext(extendContext(request.Context()).withValue(TraceActivityIDKey, activityID))
		handler.ServeHTTP(tw, newRequest)

		elapsed := time.Since(timeNow)
		user := "anonymous"
		userProvider := "none"
		if userPrincipal, ok := newRequest.Context().Value(UserPrincipalKey).(*UserPrincipal); ok {
			user = userPrincipal.ID
			userProvider = userPrincipal.Provider
		}

		zap.L().Info("requestServed",
			zap.String("type", "apiCall"),
			zap.String("correlationId", correlationID),
			zap.String("activityId", activityID),
			zap.String("requestUri", newRequest.URL.String()),
			zap.String("path", newRequest.URL.Path),
			zap.String("requestMethod", newRequest.Method),
			zap.String("user", user),
			zap.String("userProvider", userProvider),
			zap.String("remoteAddr", newRequest.RemoteAddr),
			zap.Int("responseStatus", tw.statusCode),
			zap.Int("responseBytes", tw.contentLength),
			zap.Int("latency", int(elapsed.Seconds()*1000)))
		defer zap.L().Sync()
	}

	return http.HandlerFunc(handlerFunction)
}

// LogOperation emit latency and error (if any) for the given operation
func LogOperation(ctx context.Context, operation string, operator func() error) error {
	tsStart := time.Now()
	err := operator()
	timeElapsed := time.Since(tsStart)
	activityID := GetTraceID(ctx)
	if err == nil {
		zap.L().Info("operationDone",
			zap.String("op", operation),
			zap.String("activityId", activityID),
			zap.Int("latency", int(timeElapsed.Seconds()*1000)))
	} else {
		zap.L().Error("operationFailed",
			zap.String("op", operation),
			zap.String("activityId", activityID),
			zap.Error(err),
			zap.Int("latency", int(timeElapsed.Seconds()*1000)))
	}

	return err
}
