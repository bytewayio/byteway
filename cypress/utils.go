package cypress

import (
	"regexp"
	"strings"
	"time"
)

var matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
var matchAllCap = regexp.MustCompile("([a-z0-9])([A-Z])")

// ToSnakeCase maps CamelCase to SnakeCase
func ToSnakeCase(str string) string {
	snake := matchFirstCap.ReplaceAllString(str, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
}

// GetEpochMillis get epoch milliseconds for now
func GetEpochMillis() int64 {
	return ConvertToEpochMillis(time.Now())
}

// ConvertToEpochMillis convert a time object to epoch in milliseconds
func ConvertToEpochMillis(t time.Time) int64 {
	return t.UnixNano() / (int64(time.Millisecond) / int64(time.Nanosecond))
}

func Max[T Ordered](v1, v2 T) T {
	if v1 > v2 {
		return v1
	}

	return v2
}

func Min[T Ordered](v1, v2 T) T {
	if v1 < v2 {
		return v1
	}

	return v2
}
