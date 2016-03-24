package dataframe

import (
	"strconv"
	"time"
)

// Value represents the value in data frame.
type Value interface {
	ToNumber() (float64, bool)
	ToString() (string, bool)
	ToTime(layout string) (time.Time, bool)
	ToDuration() (time.Duration, bool)
	IsNil() bool
	EqualTo(v Value) bool
}

func NewValue(v string) Value {
	return stringType(v)
}

type stringType string

func (s stringType) ToNumber() (float64, bool) {
	f, err := strconv.ParseFloat(string(s), 64)
	return f, err == nil
}

func (s stringType) ToString() (string, bool) {
	return string(s), true
}

func (s stringType) ToTime(layout string) (time.Time, bool) {
	t, err := time.Parse(layout, string(s))
	return t, err == nil
}

func (s stringType) ToDuration() (time.Duration, bool) {
	d, err := time.ParseDuration(string(s))
	return d, err == nil
}

func (s stringType) IsNil() bool {
	return len(s) == 0
}

func (s stringType) EqualTo(v Value) bool {
	tv, ok := v.(stringType)
	return ok && s == tv
}
