package utilities

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

func ConvertTFToKrakenInterval(tf string) (string, error) {
	// Map of standard timeframes to Kraken intervals
	intervalMap := map[string]string{
		"1m":  "1",
		"5m":  "5",
		"15m": "15",
		"30m": "30",
		"1h":  "60",
		"4h":  "240",
		"1d":  "1440",
		"1w":  "10080",
	}

	// Check if the input is a standard timeframe
	if interval, ok := intervalMap[strings.ToLower(tf)]; ok {
		return interval, nil
	}

	// Defensive check: if the input is already a Kraken interval, return it unchanged
	validKrakenIntervals := map[string]bool{
		"1":     true,
		"5":     true,
		"15":    true,
		"30":    true,
		"60":    true,
		"240":   true,
		"1440":  true,
		"10080": true,
	}
	if validKrakenIntervals[tf] {
		return tf, nil // Return the Kraken interval as-is to prevent re-conversion errors
	}

	// If the timeframe is unsupported, return an error
	return "", fmt.Errorf("unsupported timeframe for Kraken interval conversion: %s", tf)
}

// DoJSONRequest performs an HTTP request, retries on transient errors, and unmarshals a JSON response.
func DoJSONRequest(client *http.Client, req *http.Request, maxRetries int, retryDelay time.Duration, result interface{}) error {
	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		r := req
		if attempt > 0 && req.GetBody != nil {
			bodyReader, err := req.GetBody()
			if err != nil {
				return fmt.Errorf("retry %d: could not reset request body: %w", attempt, err)
			}
			r = req.Clone(req.Context())
			r.Body = bodyReader
		}

		resp, err := client.Do(r)
		if err != nil {
			lastErr = err
			time.Sleep(retryDelay)
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode >= 500 && resp.StatusCode <= 599 {
			lastErr = fmt.Errorf("server error %d %s", resp.StatusCode, resp.Status)
			time.Sleep(retryDelay)
			continue
		}

		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			return fmt.Errorf("http %d: %s", resp.StatusCode, string(snippet))
		}

		if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
			return fmt.Errorf("failed to decode JSON response: %w", err)
		}
		return nil
	}
	return fmt.Errorf("all retries failed: %w", lastErr)
}

// FilterAfter returns a subset of items that occur after a given cutoff time.
func FilterAfter[T any](items []T, getTime func(T) time.Time, cutoff time.Time) []T {
	var out []T
	for _, it := range items {
		if getTime(it).After(cutoff) {
			out = append(out, it)
		}
	}
	return out
}

// GenerateKrakenAuthHeaders builds the API-Key and API-Sign headers for Kraken private endpoints.
func GenerateKrakenAuthHeaders(apiKey, apiSecret, apiPath, nonce string, data url.Values) (map[string]string, error) {
	postData := data.Encode()
	sha256Hasher := sha256.New()
	sha256Hasher.Write([]byte(nonce + postData))
	shaSum := sha256Hasher.Sum(nil)

	message := append([]byte(apiPath), shaSum...)

	decodedSecret, err := base64.StdEncoding.DecodeString(apiSecret)
	if err != nil {
		return nil, err
	}

	mac := hmac.New(sha512.New, decodedSecret)
	mac.Write(message)
	macSum := mac.Sum(nil)

	signature := base64.StdEncoding.EncodeToString(macSum)

	return map[string]string{
		"API-Key":  apiKey,
		"API-Sign": signature,
	}, nil
}

// LogDebug logs a message at Debug level.
func (l *Logger) LogDebug(format string, v ...interface{}) {
	if l.Level <= Debug {
		_ = l.Logger.Output(2, fmt.Sprintf("[DEBUG] "+format, v...))
	}
}

// LogError logs a message at Error level.
func (l *Logger) LogError(format string, v ...interface{}) {
	if l.Level <= Error {
		_ = l.Logger.Output(2, fmt.Sprintf("[ERROR] "+format, v...))
	}
}

// LogFatal logs a message at Fatal level and then calls os.Exit(1).
func (l *Logger) LogFatal(format string, v ...interface{}) {
	_ = l.Logger.Output(2, fmt.Sprintf("[FATAL] "+format, v...))
	os.Exit(1)
}

// LogInfo logs a message at Info level.
func (l *Logger) LogInfo(format string, v ...interface{}) {
	if l.Level <= Info {
		_ = l.Logger.Output(2, fmt.Sprintf("[INFO] "+format, v...))
	}
}

// LogWarn logs a message at Warn level.
func (l *Logger) LogWarn(format string, v ...interface{}) {
	if l.Level <= Warn {
		_ = l.Logger.Output(2, fmt.Sprintf("[WARN] "+format, v...))
	}
}

// minInt returns the minimum of two integers.
func MinInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Next generates the next nonce value.
func (nc *InMemoryNonceCounter) Next() string {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	now := time.Now().UnixNano()
	if now <= nc.lastNonce {
		now = nc.lastNonce + 1
	}
	nc.lastNonce = now
	return strconv.FormatInt(now, 10)
}

// NewLogger creates a new Logger instance.
func NewLogger(level LogLevel) *Logger {
	return &Logger{
		Level:  level,
		Logger: log.New(os.Stdout, "[Snowballin] ", log.Ldate|log.Ltime|log.Lshortfile),
	}
}

// NewNonceCounter creates a new KrakenNonceGenerator.
func NewNonceCounter() *KrakenNonceGenerator {
	return &KrakenNonceGenerator{counter: uint64(time.Now().UnixNano())}
}

// Nonce generates and returns a new unique nonce.
func (n *KrakenNonceGenerator) Nonce() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.counter++
	return n.counter
}

// ParseFloatFromInterface is a helper function to parse float64 from various numeric types.
func ParseFloatFromInterface(val interface{}) (float64, error) {
	switch v := val.(type) {
	case string:
		return strconv.ParseFloat(v, 64)
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case json.Number:
		return v.Float64()
	default:
		return 0, fmt.Errorf("unsupported type for float conversion: %T", v)
	}
}

// ParseLogLevel converts a string log level to the LogLevel type.
func ParseLogLevel(levelStr string) (LogLevel, error) {
	switch strings.ToLower(levelStr) {
	case "debug":
		return Debug, nil
	case "info":
		return Info, nil
	case "warn":
		return Warn, nil
	case "error":
		return Error, nil
	case "fatal":
		return Fatal, nil
	default:
		return Info, fmt.Errorf("invalid log level string: %s", levelStr)
	}
}

// SetLogLevel updates the logging level of the logger.
func (l *Logger) SetLogLevel(level LogLevel) {
	l.Level = level
}

// SortBarsByTimestamp sorts a slice of OHLCVBar by ascending Timestamp.
func SortBarsByTimestamp(bars []OHLCVBar) {
	sort.Slice(bars, func(i, j int) bool {
		return bars[i].Timestamp < bars[j].Timestamp
	})
}
