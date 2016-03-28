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
		d := c.PushBack(NewStringValue(i))
		if i+1 != d {
			t.Fatalf("expected %d, got %d", i+1, d)
		}
	}
	if c.RowNumber() != 100 {
		t.Fatalf("expected '100', got %v", c.RowNumber())
	}

	if err := c.SetValue(10, NewStringValue(10000)); err != nil {
		t.Fatal(err)
	}
	if v, err := c.GetValue(10); err != nil || !v.EqualTo(NewStringValue(10000)) {
		t.Fatalf("expected '10', got %v(%v)", v, err)
	}

	if err := c.SetValue(10, NewStringValue(10)); err != nil {
		t.Fatal(err)
	}
	if v, err := c.GetValue(10); err != nil || !v.EqualTo(NewStringValue(10)) {
		t.Fatalf("expected '10', got %v(%v)", v, err)
	}
	idx, ok := c.FindValue(NewStringValue(10))
	if !ok || idx != 10 {
		t.Fatalf("expected 10, got %d", idx)
	}
	bv, ok := c.Back()
	if !ok || !bv.EqualTo(NewStringValue(99)) {
		t.Fatalf("expected '99', got %v", bv)
	}
	bv, ok = c.PopBack()
	if !ok || !bv.EqualTo(NewStringValue(99)) {
		t.Fatalf("expected '99', got %v", bv)
	}
	fv, ok := c.Front()
	if !ok || !fv.EqualTo(NewStringValue(0)) {
		t.Fatalf("expected '0', got %v", fv)
	}
	fv, ok = c.PopFront()
	if !ok || !fv.EqualTo(NewStringValue(0)) {
		t.Fatalf("expected '0', got %v", fv)
	}
	dv, err := c.DeleteRow(1)
	if err != nil || !dv.EqualTo(NewStringValue(2)) {
		t.Fatalf("expected '2', got %v(%v)", dv, err)
	}
	fidx, ok := c.FindValue(NewStringValue(2))
	if fidx != -1 || ok {
		t.Fatalf("expected -1, false, got %v %v", fidx, ok)
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
		d := c.PushBack(NewStringValue(i))
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
	if !ok || !fv.EqualTo(NewStringValue(0)) {
		t.Fatalf("expected '0', got %v", fv)
	}
	bv, ok := c.Back()
	if !ok || !bv.EqualTo(NewStringValue("")) {
		t.Fatalf("expected '99', got %v", bv)
	}
	bv, ok = c.BackNonNil()
	if !ok || !bv.EqualTo(NewStringValue(99)) {
		t.Fatalf("expected '99', got %v", bv)
	}
}

func TestColumnAppends(t *testing.T) {
	c := NewColumn("second")
	c.PushBack(NewStringValue(1))
	c.PushBack(NewStringValue(2))
	if err := c.Appends(NewStringValue(1000), 1000); err != nil {
		t.Fatal(err)
	}
	s := c.RowNumber()
	if s != 1000 {
		t.Fatalf("expected '1000', got %v", s)
	}
	fv, ok := c.Front()
	if !ok || !fv.EqualTo(NewStringValue(1)) {
		t.Fatalf("expected '1', got %v", fv)
	}
	fv, ok = c.FrontNonNil()
	if !ok || !fv.EqualTo(NewStringValue(1)) {
		t.Fatalf("expected '1', got %v", fv)
	}
	bv, ok := c.Back()
	if !ok || !bv.EqualTo(NewStringValue(1000)) {
		t.Fatalf("expected '1000', got %v", bv)
	}
	bv, ok = c.BackNonNil()
	if !ok || !bv.EqualTo(NewStringValue(1000)) {
		t.Fatalf("expected '1000', got %v", bv)
	}
}

func TestColumnAppendsNil(t *testing.T) {
	c := NewColumn("second")
	c.PushBack(NewStringValue(1))
	c.PushBack(NewStringValue(2))
	if err := c.Appends(NewStringValue(""), 1000); err != nil {
		t.Fatal(err)
	}
	s := c.RowNumber()
	if s != 1000 {
		t.Fatalf("expected '1000', got %v", s)
	}
	fv, ok := c.Front()
	if !ok || !fv.EqualTo(NewStringValue(1)) {
		t.Fatalf("expected '1', got %v", fv)
	}
	bv, ok := c.Back()
	if !ok || !bv.EqualTo(NewStringValue("")) {
		t.Fatalf("expected '', got %v", bv)
	}
}

func TestColumnDeleteRows(t *testing.T) {
	c := NewColumn("second")
	for i := 0; i < 100; i++ {
		d := c.PushBack(NewStringValue(fmt.Sprintf("%d", i)))
		if i+1 != d {
			t.Fatalf("expected %d, got %d", i+1, d)
		}
	}
	idx, ok := c.FindValue(NewStringValue(60))
	if idx != 60 || !ok {
		t.Fatalf("expected 60, true, got %d %v", idx, ok)
	}
	if err := c.DeleteRows(50, 70); err != nil {
		t.Fatal(err)
	}
	idx, ok = c.FindValue(NewStringValue(70))
	if idx != 50 || !ok {
		t.Fatalf("expected 50, true, got %d %v", idx, ok)
	}
	if c.RowNumber() != 80 {
		t.Fatalf("expected 80, got %d", c.RowNumber())
	}
	idx, ok = c.FindValue(NewStringValue(60))
	if idx != -1 || ok {
		t.Fatalf("expected -1, false, got %d %v", idx, ok)
	}
}

func TestColumnKeepRows(t *testing.T) {
	c := NewColumn("second")
	for i := 0; i < 100; i++ {
		d := c.PushBack(NewStringValue(i))
		if i+1 != d {
			t.Fatalf("expected %d, got %d", i+1, d)
		}
	}
	if err := c.KeepRows(50, 70); err != nil {
		t.Fatal(err)
	}
	idx, ok := c.FindValue(NewStringValue(50))
	if idx != 0 || !ok {
		t.Fatalf("expected 0, true, got %d %v", idx, ok)
	}
	idx, ok = c.FindValue(NewStringValue(69))
	if idx != 19 || !ok {
		t.Fatalf("expected 19, true, got %d %v", idx, ok)
	}
	idx, ok = c.FindValue(NewStringValue(70))
	if idx != -1 || ok {
		t.Fatalf("expected -1, false, got %d %v", idx, ok)
	}
	if c.RowNumber() != 20 {
		t.Fatalf("expected 20, got %d", c.RowNumber())
	}
	idx, ok = c.FindValue(NewStringValue(90))
	if idx != -1 || ok {
		t.Fatalf("expected -1, false, got %d %v", idx, ok)
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
	if !fv.EqualTo(NewStringValue(0)) {
		t.Fatalf("expected '0', got %v", fv)
	}
}

func TestColumnSortByStringDescending(t *testing.T) {
	c := NewColumn("column")
	for i := 0; i < 100; i++ {
		c.PushBack(NewStringValue(i))
	}
	c.SortByStringDescending()
	fv, err := c.GetValue(0)
	if err != nil {
		t.Fatal(err)
	}
	if !fv.EqualTo(NewStringValue(99)) {
		t.Fatalf("expected '99', got %v", fv)
	}
}

func TestColumnSortByNumberAscending(t *testing.T) {
	c := NewColumn("column")
	for i := 0; i < 100; i++ {
		c.PushBack(NewStringValue(i))
	}
	c.SortByNumberAscending()
	fv, err := c.GetValue(0)
	if err != nil {
		t.Fatal(err)
	}
	if !fv.EqualTo(NewStringValue(0)) {
		t.Fatalf("expected '0', got %v", fv)
	}
}

func TestColumnSortByNumberDescending(t *testing.T) {
	c := NewColumn("column")
	for i := 0; i < 100; i++ {
		c.PushBack(NewStringValue(i))
	}
	c.PushBack(NewStringValue("199.9"))
	c.SortByNumberDescending()
	fv, err := c.GetValue(0)
	if err != nil {
		t.Fatal(err)
	}
	if !fv.EqualTo(NewStringValue(199.9)) {
		t.Fatalf("expected '199.9', got %v", fv)
	}
}

func TestColumnSortByDurationAscending(t *testing.T) {
	c := NewColumn("column")
	for i := 0; i < 100; i++ {
		c.PushBack(NewStringValue(time.Duration(i) * time.Second))
	}
	c.SortByDurationAscending()
	fv, err := c.GetValue(0)
	if err != nil {
		t.Fatal(err)
	}
	if !fv.EqualTo(NewStringValue(0)) {
		t.Fatalf("expected '0', got %v", fv)
	}
}

func TestColumnSortByDurationDescending(t *testing.T) {
	c := NewColumn("column")
	for i := 0; i < 100; i++ {
		c.PushBack(NewStringValue(time.Duration(i) * time.Second))
	}
	c.PushBack(NewStringValue(200 * time.Hour))
	c.SortByDurationDescending()
	fv, err := c.GetValue(0)
	if err != nil {
		t.Fatal(err)
	}
	if !fv.EqualTo(NewStringValue(200 * time.Hour)) {
		t.Fatalf("expected '200h', got %v", fv)
	}
}
