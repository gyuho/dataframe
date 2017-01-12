package dataframe

import (
	"fmt"
	"sort"
	"testing"
	"time"
)

func TestValue(t *testing.T) {
	v1 := NewStringValue("1")
	if v, ok := v1.Number(); !ok {
		t.Fatalf("expected number 1, got %v", v)
	}

	v2 := NewStringValue("2.2")
	if v, ok := v2.Number(); !ok || v != 2.2 {
		t.Fatalf("expected number 2.2, got %v", v)
	}

	v2c := v2.Copy()
	if !v2.EqualTo(v2c) {
		t.Fatalf("expected equal, got %v", v2.EqualTo(v2c))
	}

	v3t := time.Now().String()
	v3 := NewStringValue(v3t)
	if v, ok := v3.Time("2006-01-02 15:04:05 -0700 MST"); !ok {
		t.Fatalf("expected time %s, got %v", v3t, v)
	}

	v4t := time.Now().String()[:19]
	v4 := NewStringValue(v4t)
	if v, ok := v4.Time("2006-01-02 15:04:05"); !ok {
		t.Fatalf("expected time %s, got %v", v4t, v)
	}

	if !NewStringValue("hello").EqualTo(NewStringValue("hello")) {
		t.Fatal("EqualTo expected 'true' for 'hello' == 'hello' but got false")
	}
}

func TestNewStringValueNil(t *testing.T) {
	v := NewStringValueNil()
	if !v.IsNil() {
		t.Fatalf("expected nil, got %v", v)
	}
}

func TestByStringAscending(t *testing.T) {
	vs := []Value{}
	for i := 0; i < 100; i++ {
		vs = append(vs, NewStringValue(fmt.Sprintf("%d", i)))
	}
	sort.Sort(ByStringAscending(vs))
	if !vs[0].EqualTo(NewStringValue("0")) {
		t.Fatalf("expected '0', got %v", vs[0])
	}
}

func TestByStringDescending(t *testing.T) {
	vs := []Value{}
	for i := 0; i < 100; i++ {
		vs = append(vs, NewStringValue(fmt.Sprintf("%d", i)))
	}
	sort.Sort(ByStringDescending(vs))
	if !vs[0].EqualTo(NewStringValue("99")) {
		t.Fatalf("expected '99', got %v", vs[0])
	}
}

func TestByNumberAscending(t *testing.T) {
	vs := []Value{}
	for i := 0; i < 100; i++ {
		vs = append(vs, NewStringValue(fmt.Sprintf("%d", i)))
	}
	sort.Sort(ByNumberAscending(vs))
	if !vs[0].EqualTo(NewStringValue("0")) {
		t.Fatalf("expected '0', got %v", vs[0])
	}
}

func TestByNumberDescending(t *testing.T) {
	vs := []Value{}
	for i := 0; i < 100; i++ {
		vs = append(vs, NewStringValue(fmt.Sprintf("%d", i)))
	}
	vs = append(vs, NewStringValue("199.9"))
	sort.Sort(ByNumberDescending(vs))
	if !vs[0].EqualTo(NewStringValue("199.9")) {
		t.Fatalf("expected '199.9', got %v", vs[0])
	}
}

func TestByDurationAscending(t *testing.T) {
	vs := []Value{}
	for i := 0; i < 100; i++ {
		vs = append(vs, NewStringValue(fmt.Sprintf("%s", time.Duration(i)*time.Second)))
	}
	sort.Sort(ByDurationAscending(vs))
	if !vs[0].EqualTo(NewStringValue("0s")) {
		t.Fatalf("expected '0s', got %v", vs[0])
	}
}

func TestByDurationDescending(t *testing.T) {
	vs := []Value{}
	for i := 0; i < 100; i++ {
		vs = append(vs, NewStringValue(fmt.Sprintf("%s", time.Duration(i)*time.Second)))
	}
	vs = append(vs, NewStringValue("200h"))
	sort.Sort(ByDurationDescending(vs))
	if !vs[0].EqualTo(NewStringValue("200h")) {
		t.Fatalf("expected '200h', got %v", vs[0])
	}
}
