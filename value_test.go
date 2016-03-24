package dataframe

import (
	"testing"
	"time"
)

func TestValue(t *testing.T) {
	v1 := NewValue("1")
	if v, ok := v1.ToNumber(); !ok {
		t.Fatalf("expected number 1, got %v", v)
	}

	v2 := NewValue("2.2")
	if v, ok := v2.ToNumber(); !ok || v != 2.2 {
		t.Fatalf("expected number 2.2, got %v", v)
	}

	v3t := time.Now().String()
	v3 := NewValue(v3t)
	if v, ok := v3.ToTime("2006-01-02 15:04:05 -0700 MST"); !ok {
		t.Fatalf("expected time %s, got %v", v3t, v)
	}

	v4t := time.Now().String()[:19]
	v4 := NewValue(v4t)
	if v, ok := v4.ToTime("2006-01-02 15:04:05"); !ok {
		t.Fatalf("expected time %s, got %v", v4t, v)
	}

	if !NewValue("hello").EqualTo(NewValue("hello")) {
		t.Fatal("EqualTo expected 'true' for 'hello' == 'hello' but got false")
	}
}
