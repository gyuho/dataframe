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

	// Number parses Value to float64. It returns false if not possible.
	Number() (float64, bool)

	// Time parses Value to time.Time based on the layout. It returns false if not possible.
	Time(layout string) (time.Time, bool)

	// Duration parses Value to time.Duration. It returns false if not possible.
	Duration() (time.Duration, bool)

	// IsNil returns true if the Value is nil.
	IsNil() bool

	// EqualTo returns true if the Value is equal to v.
	EqualTo(v Value) bool
}

func NewStringValue(v interface{}) Value {
	switch t := v.(type) {
	case string:
		return String(t)
	case int:
		return String(strconv.FormatInt(int64(t), 10))
	case float64:
		return String(strconv.FormatFloat(t, 'f', -1, 64))
	case time.Time:
		return String(t.String())
	case time.Duration:
		return String(t.String())
	default:
		panic(fmt.Errorf("%v is not supported", v))
	}
	return nil
}

func NewStringValueNil() Value {
	return String("")
}

type String string

func (s String) String() (string, bool) {
	return string(s), true
}

func (s String) Number() (float64, bool) {
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

type ByNumberAscending []Value

func (vs ByNumberAscending) Len() int {
	return len(vs)
}

func (vs ByNumberAscending) Swap(i, j int) {
	vs[i], vs[j] = vs[j], vs[i]
}

func (vs ByNumberAscending) Less(i, j int) bool {
	vs1, _ := vs[i].Number()
	vs2, _ := vs[j].Number()
	return vs1 < vs2
}

type ByNumberDescending []Value

func (vs ByNumberDescending) Len() int {
	return len(vs)
}

func (vs ByNumberDescending) Swap(i, j int) {
	vs[i], vs[j] = vs[j], vs[i]
}

func (vs ByNumberDescending) Less(i, j int) bool {
	vs1, _ := vs[i].Number()
	vs2, _ := vs[j].Number()
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
