package dataframe

import (
	"fmt"
	"testing"
	"time"
)

func TestColumn(t *testing.T) {
	c := NewColumn("second")
	if c.GetHeader() != "second" {
		t.Fatalf("expected 'second', got %v", c.GetHeader())
	}
	for i := 0; i < 100; i++ {
		d := c.PushBack(NewValue(fmt.Sprintf("%d", i)))
		if i+1 != d {
			t.Fatalf("expected %d, got %d", i+1, d)
		}
	}
	if c.Len() != 100 {
		t.Fatalf("expected '100', got %v", c.Len())
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

func TestColumnByStringAscending(t *testing.T) {
	c := NewColumn("column")
	for i := 0; i < 100; i++ {
		c.PushBack(NewValue(fmt.Sprintf("%d", i)))
	}
	c.ByStringAscending()
	fv, err := c.GetValue(0)
	if err != nil {
		t.Fatal(err)
	}
	if !fv.EqualTo(NewValue("0")) {
		t.Fatalf("expected '0', got %v", fv)
	}
}

func TestColumnByStringDescending(t *testing.T) {
	c := NewColumn("column")
	for i := 0; i < 100; i++ {
		c.PushBack(NewValue(fmt.Sprintf("%d", i)))
	}
	c.ByStringDescending()
	fv, err := c.GetValue(0)
	if err != nil {
		t.Fatal(err)
	}
	if !fv.EqualTo(NewValue("99")) {
		t.Fatalf("expected '99', got %v", fv)
	}
}

func TestColumnByNumberAscending(t *testing.T) {
	c := NewColumn("column")
	for i := 0; i < 100; i++ {
		c.PushBack(NewValue(fmt.Sprintf("%d", i)))
	}
	c.ByNumberAscending()
	fv, err := c.GetValue(0)
	if err != nil {
		t.Fatal(err)
	}
	if !fv.EqualTo(NewValue("0")) {
		t.Fatalf("expected '0', got %v", fv)
	}
}

func TestColumnByNumberDescending(t *testing.T) {
	c := NewColumn("column")
	for i := 0; i < 100; i++ {
		c.PushBack(NewValue(fmt.Sprintf("%d", i)))
	}
	c.PushBack(NewValue("199.9"))
	c.ByNumberDescending()
	fv, err := c.GetValue(0)
	if err != nil {
		t.Fatal(err)
	}
	if !fv.EqualTo(NewValue("199.9")) {
		t.Fatalf("expected '199.9', got %v", fv)
	}
}

func TestColumnByDurationAscending(t *testing.T) {
	c := NewColumn("column")
	for i := 0; i < 100; i++ {
		c.PushBack(NewValue(fmt.Sprintf("%s", time.Duration(i)*time.Second)))
	}
	c.ByDurationAscending()
	fv, err := c.GetValue(0)
	if err != nil {
		t.Fatal(err)
	}
	if !fv.EqualTo(NewValue("0")) {
		t.Fatalf("expected '0', got %v", fv)
	}
}

func TestColumnByDurationDescending(t *testing.T) {
	c := NewColumn("column")
	for i := 0; i < 100; i++ {
		c.PushBack(NewValue(fmt.Sprintf("%s", time.Duration(i)*time.Second)))
	}
	c.PushBack(NewValue("200h"))
	c.ByDurationDescending()
	fv, err := c.GetValue(0)
	if err != nil {
		t.Fatal(err)
	}
	if !fv.EqualTo(NewValue("200h")) {
		t.Fatalf("expected '200h', got %v", fv)
	}
}
