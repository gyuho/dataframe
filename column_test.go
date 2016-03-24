package dataframe

import (
	"fmt"
	"testing"
)

func TestColumn(t *testing.T) {
	c := NewColumn("second")
	if c.GetHeader() != "second" {
		t.Fatalf("expected 'second', got %v", c.GetHeader())
	}
	for i := 0; i < 100; i++ {
		d := c.PushBack(NewValue(fmt.Sprintf("%d", i)))
		if uint64(i+1) != d {
			t.Fatalf("expected %d, got %d", i+1, d)
		}
	}
	if c.GetSize() != 100 {
		t.Fatalf("expected '100', got %v", c.GetSize())
	}
	if v, err := c.GetValue(10); err != nil || !v.EqualTo(NewValue(fmt.Sprintf("%d", 10))) {
		t.Fatalf("expected '10', got %v(%v)", v, err)
	}
	bv, ok := c.PopBack()
	if !ok || !bv.EqualTo(NewValue(fmt.Sprintf("%d", 99))) {
		t.Fatalf("expected '99', got %v", bv)
	}
	fv, ok := c.PopFront()
	if !ok || !fv.EqualTo(NewValue(fmt.Sprintf("%d", 0))) {
		t.Fatalf("expected '0', got %v", fv)
	}
	dv, err := c.DeleteRow(1)
	if err != nil || !dv.EqualTo(NewValue(fmt.Sprintf("%d", 2))) {
		t.Fatalf("expected '2', got %v(%v)", dv, err)
	}

	if pv := c.PushFront(NewValue("A")); pv != 98 {
		t.Fatalf("expected '98', got %v", pv)
	}
	if vv, ok := c.PopFront(); !ok || !vv.EqualTo(NewValue("A")) {
		t.Fatalf("expected 'A', got %v", vv)
	}
}
