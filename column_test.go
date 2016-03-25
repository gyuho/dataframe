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
		d := c.PushBack(NewStringValue(fmt.Sprintf("%d", i)))
		if i+1 != d {
			t.Fatalf("expected %d, got %d", i+1, d)
		}
	}
	if c.Len() != 100 {
		t.Fatalf("expected '100', got %v", c.Len())
	}
	if v, err := c.GetValue(10); err != nil || !v.EqualTo(NewStringValue(fmt.Sprintf("%d", 10))) {
		t.Fatalf("expected '10', got %v(%v)", v, err)
	}
	bv, ok := c.Back()
	if !ok || !bv.EqualTo(NewStringValue(fmt.Sprintf("%d", 99))) {
		t.Fatalf("expected '99', got %v", bv)
	}
	bv, ok = c.PopBack()
	if !ok || !bv.EqualTo(NewStringValue(fmt.Sprintf("%d", 99))) {
		t.Fatalf("expected '99', got %v", bv)
	}
	fv, ok := c.Front()
	if !ok || !fv.EqualTo(NewStringValue(fmt.Sprintf("%d", 0))) {
		t.Fatalf("expected '0', got %v", fv)
	}
	fv, ok = c.PopFront()
	if !ok || !fv.EqualTo(NewStringValue(fmt.Sprintf("%d", 0))) {
		t.Fatalf("expected '0', got %v", fv)
	}
	dv, err := c.DeleteRow(1)
	if err != nil || !dv.EqualTo(NewStringValue(fmt.Sprintf("%d", 2))) {
		t.Fatalf("expected '2', got %v(%v)", dv, err)
	}

	if pv := c.PushFront(NewStringValue("A")); pv != 98 {
		t.Fatalf("expected '98', got %v", pv)
	}
	if vv, ok := c.PopFront(); !ok || !vv.EqualTo(NewStringValue("A")) {
		t.Fatalf("expected 'A', got %v", vv)
	}
}

func TestColumnNonNil(t *testing.T) {
	c := NewColumn("second")
	if c.GetHeader() != "second" {
		t.Fatalf("expected 'second', got %v", c.GetHeader())
	}
	for i := 0; i < 100; i++ {
		d := c.PushBack(NewStringValue(fmt.Sprintf("%d", i)))
		if i+1 != d {
			t.Fatalf("expected %d, got %d", i+1, d)
		}
	}
	c.PushFront(NewStringValue(""))
	c.PushBack(NewStringValue(""))

	fv, ok := c.Front()
	if !ok || !fv.EqualTo(NewStringValue("")) {
		t.Fatalf("expected '0', got %v", fv)
	}
	fv, ok = c.FrontNonNil()
	if !ok || !fv.EqualTo(NewStringValue(fmt.Sprintf("%d", 0))) {
		t.Fatalf("expected '0', got %v", fv)
	}
	bv, ok := c.Back()
	if !ok || !bv.EqualTo(NewStringValue("")) {
		t.Fatalf("expected '99', got %v", bv)
	}
	bv, ok = c.BackNonNil()
	if !ok || !bv.EqualTo(NewStringValue(fmt.Sprintf("%d", 99))) {
		t.Fatalf("expected '99', got %v", bv)
	}
}

func TestColumnAppends(t *testing.T) {
	c := NewColumn("second")
	c.PushBack(NewStringValue("1"))
	c.PushBack(NewStringValue("2"))
	if err := c.Appends(NewStringValue("1000"), 1000); err != nil {
		t.Fatal(err)
	}
	s := c.Len()
	if s != 1000 {
		t.Fatalf("expected '1000', got %v", s)
	}
	fv, ok := c.Front()
	if !ok || !fv.EqualTo(NewStringValue("1")) {
		t.Fatalf("expected '1', got %v", fv)
	}
	fv, ok = c.FrontNonNil()
	if !ok || !fv.EqualTo(NewStringValue("1")) {
		t.Fatalf("expected '1', got %v", fv)
	}
	bv, ok := c.Back()
	if !ok || !bv.EqualTo(NewStringValue("1000")) {
		t.Fatalf("expected '1000', got %v", bv)
	}
	bv, ok = c.BackNonNil()
	if !ok || !bv.EqualTo(NewStringValue("1000")) {
		t.Fatalf("expected '1000', got %v", bv)
	}
}

func TestColumnAppendsNil(t *testing.T) {
	c := NewColumn("second")
	c.PushBack(NewStringValue("1"))
	c.PushBack(NewStringValue("2"))
	if err := c.Appends(NewStringValue(""), 1000); err != nil {
		t.Fatal(err)
	}
	s := c.Len()
	if s != 1000 {
		t.Fatalf("expected '1000', got %v", s)
	}
	fv, ok := c.Front()
	if !ok || !fv.EqualTo(NewStringValue("1")) {
		t.Fatalf("expected '1', got %v", fv)
	}
	bv, ok := c.Back()
	if !ok || !bv.EqualTo(NewStringValue("")) {
		t.Fatalf("expected '', got %v", bv)
	}
}

func TestColumnSortByStringAscending(t *testing.T) {
	c := NewColumn("column")
	for i := 0; i < 100; i++ {
		c.PushBack(NewStringValue(fmt.Sprintf("%d", i)))
	}
	c.SortByStringAscending()
	fv, err := c.GetValue(0)
	if err != nil {
		t.Fatal(err)
	}
	if !fv.EqualTo(NewStringValue("0")) {
		t.Fatalf("expected '0', got %v", fv)
	}
}

func TestColumnSortByStringDescending(t *testing.T) {
	c := NewColumn("column")
	for i := 0; i < 100; i++ {
		c.PushBack(NewStringValue(fmt.Sprintf("%d", i)))
	}
	c.SortByStringDescending()
	fv, err := c.GetValue(0)
	if err != nil {
		t.Fatal(err)
	}
	if !fv.EqualTo(NewStringValue("99")) {
		t.Fatalf("expected '99', got %v", fv)
	}
}

func TestColumnSortByNumberAscending(t *testing.T) {
	c := NewColumn("column")
	for i := 0; i < 100; i++ {
		c.PushBack(NewStringValue(fmt.Sprintf("%d", i)))
	}
	c.SortByNumberAscending()
	fv, err := c.GetValue(0)
	if err != nil {
		t.Fatal(err)
	}
	if !fv.EqualTo(NewStringValue("0")) {
		t.Fatalf("expected '0', got %v", fv)
	}
}

func TestColumnSortByNumberDescending(t *testing.T) {
	c := NewColumn("column")
	for i := 0; i < 100; i++ {
		c.PushBack(NewStringValue(fmt.Sprintf("%d", i)))
	}
	c.PushBack(NewStringValue("199.9"))
	c.SortByNumberDescending()
	fv, err := c.GetValue(0)
	if err != nil {
		t.Fatal(err)
	}
	if !fv.EqualTo(NewStringValue("199.9")) {
		t.Fatalf("expected '199.9', got %v", fv)
	}
}

func TestColumnSortByDurationAscending(t *testing.T) {
	c := NewColumn("column")
	for i := 0; i < 100; i++ {
		c.PushBack(NewStringValue(fmt.Sprintf("%s", time.Duration(i)*time.Second)))
	}
	c.SortByDurationAscending()
	fv, err := c.GetValue(0)
	if err != nil {
		t.Fatal(err)
	}
	if !fv.EqualTo(NewStringValue("0")) {
		t.Fatalf("expected '0', got %v", fv)
	}
}

func TestColumnSortByDurationDescending(t *testing.T) {
	c := NewColumn("column")
	for i := 0; i < 100; i++ {
		c.PushBack(NewStringValue(fmt.Sprintf("%s", time.Duration(i)*time.Second)))
	}
	c.PushBack(NewStringValue("200h"))
	c.SortByDurationDescending()
	fv, err := c.GetValue(0)
	if err != nil {
		t.Fatal(err)
	}
	if !fv.EqualTo(NewStringValue("200h")) {
		t.Fatalf("expected '200h', got %v", fv)
	}
}
