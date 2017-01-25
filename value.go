package dataframe

import (
	"fmt"
	"strconv"
	"time"
)

// Value represents the value in data frame.
type Value interface {
	// String parses Value to string. It returns false if not possible.
	String() (string, bool)

	// Int64 parses Value to int64. It returns false if not possible.
	Int64() (int64, bool)

	// Uint64 parses Value to uint64. It returns false if not possible.
	Uint64() (uint64, bool)

	// Float64 parses Value to float64. It returns false if not possible.
	Float64() (float64, bool)

	// Time parses Value to time.Time based on the layout. It returns false if not possible.
	Time(layout string) (time.Time, bool)

	// Duration parses Value to time.Duration. It returns false if not possible.
	Duration() (time.Duration, bool)

	// IsNil returns true if the Value is nil.
	IsNil() bool

	// EqualTo returns true if the Value is equal to v.
	EqualTo(v Value) bool

	// Copy copies Value.
	Copy() Value
}

// NewStringValue takes any interface and returns Value.
func NewStringValue(v interface{}) Value {
	switch t := v.(type) {
	case string:
		return String(t)
	case int:
		return String(strconv.FormatInt(int64(t), 10))
	case int64:
		return String(strconv.FormatInt(t, 10))
	case uint:
		return String(strconv.FormatUint(uint64(t), 10))
	case uint64:
		return String(strconv.FormatUint(t, 10))
	case float64:
		return String(strconv.FormatFloat(t, 'f', -1, 64))
	case time.Time:
		return String(t.String())
	case time.Duration:
		return String(t.String())
	default:
		panic(fmt.Errorf("%v(%T) is not supported", v, v))
	}
	return nil
}

// NewStringValueNil returns an empty value.
func NewStringValueNil() Value {
	return String("")
}

// String defines string data types.
type String string

func (s String) String() (string, bool) {
	return string(s), true
}

func (s String) Int64() (int64, bool) {
	iv, err := strconv.ParseInt(string(s), 10, 64)
	return iv, err == nil
}

func (s String) Uint64() (uint64, bool) {
	iv, err := strconv.ParseUint(string(s), 10, 64)
	return iv, err == nil
}

func (s String) Float64() (float64, bool) {
	f, err := strconv.ParseFloat(string(s), 64)
	return f, err == nil
}

func (s String) Time(layout string) (time.Time, bool) {
	t, err := time.Parse(layout, string(s))
	return t, err == nil
}

func (s String) Duration() (time.Duration, bool) {
	d, err := time.ParseDuration(string(s))
	return d, err == nil
}

func (s String) IsNil() bool {
	return len(s) == 0
}

func (s String) EqualTo(v Value) bool {
	tv, ok := v.(String)
	return ok && s == tv
}

func (s String) Copy() Value {
	return s
}

type ByStringAscending []Value

func (vs ByStringAscending) Len() int {
	return len(vs)
}

func (vs ByStringAscending) Swap(i, j int) {
	vs[i], vs[j] = vs[j], vs[i]
}

func (vs ByStringAscending) Less(i, j int) bool {
	vs1, _ := vs[i].String()
	vs2, _ := vs[j].String()
	return vs1 < vs2
}

type ByStringDescending []Value

func (vs ByStringDescending) Len() int {
	return len(vs)
}

func (vs ByStringDescending) Swap(i, j int) {
	vs[i], vs[j] = vs[j], vs[i]
}

func (vs ByStringDescending) Less(i, j int) bool {
	vs1, _ := vs[i].String()
	vs2, _ := vs[j].String()
	return vs1 > vs2
}

type ByFloat64Ascending []Value

func (vs ByFloat64Ascending) Len() int {
	return len(vs)
}

func (vs ByFloat64Ascending) Swap(i, j int) {
	vs[i], vs[j] = vs[j], vs[i]
}

func (vs ByFloat64Ascending) Less(i, j int) bool {
	vs1, _ := vs[i].Float64()
	vs2, _ := vs[j].Float64()
	return vs1 < vs2
}

type ByFloat64Descending []Value

func (vs ByFloat64Descending) Len() int {
	return len(vs)
}

func (vs ByFloat64Descending) Swap(i, j int) {
	vs[i], vs[j] = vs[j], vs[i]
}

func (vs ByFloat64Descending) Less(i, j int) bool {
	vs1, _ := vs[i].Float64()
	vs2, _ := vs[j].Float64()
	return vs1 > vs2
}

type ByDurationAscending []Value

func (vs ByDurationAscending) Len() int {
	return len(vs)
}

func (vs ByDurationAscending) Swap(i, j int) {
	vs[i], vs[j] = vs[j], vs[i]
}

func (vs ByDurationAscending) Less(i, j int) bool {
	vs1, _ := vs[i].Duration()
	vs2, _ := vs[j].Duration()
	return vs1 < vs2
}

type ByDurationDescending []Value

func (vs ByDurationDescending) Len() int {
	return len(vs)
}

func (vs ByDurationDescending) Swap(i, j int) {
	vs[i], vs[j] = vs[j], vs[i]
}

func (vs ByDurationDescending) Less(i, j int) bool {
	vs1, _ := vs[i].Duration()
	vs2, _ := vs[j].Duration()
	return vs1 > vs2
}
