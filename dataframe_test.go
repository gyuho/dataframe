package dataframe

import (
	"fmt"
	"os"
	"reflect"
	"testing"
)

func TestFrame(t *testing.T) {
	c1 := NewColumn("second1")
	for i := 0; i < 100; i++ {
		d := c1.PushBack(NewValue(fmt.Sprintf("%d", i)))
		if i+1 != d {
			t.Fatalf("expected %d, got %d", i+1, d)
		}
	}
	c1.UpdateHeader("aaa")
	if c1.GetHeader() != "aaa" {
		t.Fatalf("expected 'aaa', got %v", c1.GetHeader())
	}
	c1.UpdateHeader("second1")
	if c1.GetHeader() != "second1" {
		t.Fatalf("expected 'second1', got %v", c1.GetHeader())
	}

	c2 := NewColumn("second2")
	for i := 0; i < 100; i++ {
		d := c2.PushBack(NewValue(fmt.Sprintf("%d", i)))
		if i+1 != d {
			t.Fatalf("expected %d, got %d", i+1, d)
		}
	}

	fr := New()
	if err := fr.AddColumn(c1); err != nil {
		t.Fatal(err)
	}
	if err := fr.AddColumn(c1); err == nil {
		t.Fatal("expected error")
	}
	if err := fr.AddColumn(c2); err != nil {
		t.Fatal(err)
	}
	if err := fr.AddColumn(c2); err == nil {
		t.Fatal("expected error")
	}

	if c, err := fr.GetColumn("second1"); c == nil || err != nil {
		t.Fatal(err)
	}
	if c, err := fr.GetColumn("second2"); c == nil || err != nil {
		t.Fatal(err)
	}

	if err := fr.UpdateHeader("second2", "aaa"); err != nil {
		t.Fatal(err)
	}
	if c, err := fr.GetColumn("aaa"); c == nil || err != nil {
		t.Fatal(err)
	}
	if hs := fr.GetHeader(); !reflect.DeepEqual(hs, []string{"second1", "aaa"}) {
		t.Fatalf("expected equal, got %q != %q", hs, []string{"second1", "aaa"})
	}

	if err := fr.UpdateHeader("aaa", "second2"); err != nil {
		t.Fatal(err)
	}
	if hs := fr.GetHeader(); !reflect.DeepEqual(hs, []string{"second1", "second2"}) {
		t.Fatalf("expected equal, got %q != %q", hs, []string{"second1", "second2"})
	}
	if ok := fr.DeleteColumn("second1"); !ok {
		t.Fatalf("expected 'true', got %v", ok)
	}
	if cd := fr.GetColumnNumber(); cd != 1 {
		t.Fatalf("expected 1, got %v", cd)
	}
	if ok := fr.DeleteColumn("second1"); ok {
		t.Fatalf("expected 'false', got %v", ok)
	}
	if c, err := fr.GetColumn("second1"); c != nil || err == nil {
		t.Fatal("expected <nil, 'second1 does not exist'>, but <%v, %v>", c, err)
	}
	if ok := fr.DeleteColumn("second2"); !ok {
		t.Fatalf("expected 'true', got %v", ok)
	}
	if cd := fr.GetColumnNumber(); cd != 0 {
		t.Fatalf("expected 0, got %v", cd)
	}
}

func TestNewFromCSV(t *testing.T) {
	if _, err := NewFromCSV([]string{"second"}, "testdata/bench-compared.csv"); err == nil {
		t.Fatal("expected error, got nil")
	}
	fr, err := NewFromCSV(nil, "testdata/bench-compared.csv")
	if err != nil {
		t.Fatal(err)
	}
	cols := []string{"second", "avg_latency_ms_consul", "throughput_consul", "avg_cpu_consul", "avg_memory_mb_consul", "avg_latency_ms_etcd", "throughput_etcd", "avg_cpu_etcd", "avg_memory_mb_etcd", "avg_latency_ms_etcd2", "throughput_etcd2", "avg_cpu_etcd2", "avg_memory_mb_etcd2", "avg_latency_ms_zk", "throughput_zk", "avg_cpu_zk", "avg_memory_mb_zk"}
	if !reflect.DeepEqual(fr.GetHeader(), cols) {
		t.Fatalf("expected %q, got %q", cols, fr.GetHeader())
	}
	ac, err := fr.GetColumn("avg_latency_ms_etcd")
	if err != nil {
		t.Fatal(err)
	}
	if v, err := ac.GetValue(77); !v.IsNil() || err != nil {
		t.Fatalf("expected <nil, nil>, got <%v, %v>", v.IsNil(), err)
	}
	if v, err := ac.GetValue(0); v.IsNil() || !v.EqualTo(NewValue("51.607760")) || err != nil {
		t.Fatalf("expected <nil, nil>, got <%v, %v>", v, err)
	}
	ac2, err := fr.GetColumn("avg_latency_ms_etcd2")
	if err != nil {
		t.Fatal(err)
	}
	if ac.Len() != ac2.Len() {
		t.Fatalf("expected equal %v != %v", ac.Len(), ac2.Len())
	}

	fpath := "test.csv"
	if err := fr.ToCSV(fpath); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(fpath)

	{
		if _, err := NewFromCSV([]string{"second"}, fpath); err == nil {
			t.Fatal("expected error, got nil")
		}
		fr, err := NewFromCSV(nil, fpath)
		if err != nil {
			t.Fatal(err)
		}
		cols := []string{"second", "avg_latency_ms_consul", "throughput_consul", "avg_cpu_consul", "avg_memory_mb_consul", "avg_latency_ms_etcd", "throughput_etcd", "avg_cpu_etcd", "avg_memory_mb_etcd", "avg_latency_ms_etcd2", "throughput_etcd2", "avg_cpu_etcd2", "avg_memory_mb_etcd2", "avg_latency_ms_zk", "throughput_zk", "avg_cpu_zk", "avg_memory_mb_zk"}
		if !reflect.DeepEqual(fr.GetHeader(), cols) {
			t.Fatalf("expected %q, got %q", cols, fr.GetHeader())
		}
		ac, err := fr.GetColumn("avg_latency_ms_etcd")
		if err != nil {
			t.Fatal(err)
		}
		if v, err := ac.GetValue(77); !v.IsNil() || err != nil {
			t.Fatalf("expected <nil, nil>, got <%v, %v>", v.IsNil(), err)
		}
		if v, err := ac.GetValue(0); v.IsNil() || !v.EqualTo(NewValue("51.607760")) || err != nil {
			t.Fatalf("expected <nil, nil>, got <%v, %v>", v, err)
		}
		ac2, err := fr.GetColumn("avg_latency_ms_etcd2")
		if err != nil {
			t.Fatal(err)
		}
		if ac.Len() != ac2.Len() {
			t.Fatalf("expected equal %v != %v", ac.Len(), ac2.Len())
		}
	}
}

func TestNewFromRows(t *testing.T) {
	fr, err := NewFromCSV(nil, "testdata/bench-compared.csv")
	if err != nil {
		t.Fatal(err)
	}
	header, rows := fr.ToRows()
	fr2, err := NewFromRows(header, rows)
	if err != nil {
		t.Fatal(err)
	}
	fpath := "test.csv"
	if err := fr2.ToCSV(fpath); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(fpath)

	fr, err = NewFromCSV(nil, fpath)
	if err != nil {
		t.Fatal(err)
	}
	cols := []string{"second", "avg_latency_ms_consul", "throughput_consul", "avg_cpu_consul", "avg_memory_mb_consul", "avg_latency_ms_etcd", "throughput_etcd", "avg_cpu_etcd", "avg_memory_mb_etcd", "avg_latency_ms_etcd2", "throughput_etcd2", "avg_cpu_etcd2", "avg_memory_mb_etcd2", "avg_latency_ms_zk", "throughput_zk", "avg_cpu_zk", "avg_memory_mb_zk"}
	if !reflect.DeepEqual(fr.GetHeader(), cols) {
		t.Fatalf("expected %q, got %q", cols, fr.GetHeader())
	}
	ac, err := fr.GetColumn("avg_latency_ms_etcd")
	if err != nil {
		t.Fatal(err)
	}
	if v, err := ac.GetValue(77); !v.IsNil() || err != nil {
		t.Fatalf("expected <nil, nil>, got <%v, %v>", v.IsNil(), err)
	}
	if v, err := ac.GetValue(0); v.IsNil() || !v.EqualTo(NewValue("51.607760")) || err != nil {
		t.Fatalf("expected <nil, nil>, got <%v, %v>", v, err)
	}
	ac2, err := fr.GetColumn("avg_latency_ms_etcd2")
	if err != nil {
		t.Fatal(err)
	}
	if ac.Len() != ac2.Len() {
		t.Fatalf("expected equal %v != %v", ac.Len(), ac2.Len())
	}
}

func TestSort(t *testing.T) {
	fr, err := NewFromCSV(nil, "testdata/bench-compared.csv")
	if err != nil {
		t.Fatal(err)
	}
	if err := fr.Sort("second", SortType_Number, SortOption_Descending); err != nil {
		t.Fatal(err)
	}
	fpath := "test.csv"
	if err := fr.ToCSV(fpath); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(fpath)
}
