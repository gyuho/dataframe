package dataframe

import (
	"fmt"
	"sync"
)

// Column represents column-based data.
type Column interface {
	GetHeader() string
	GetSize() uint64
	GetValue(row uint64) (Value, error)
	PushFront(v Value) uint64
	PushBack(v Value) uint64
	DeleteRow(row uint64) (Value, error)
	PopFront() (Value, bool)
	PopBack() (Value, bool)
}

type column struct {
	mu     sync.Mutex
	header string
	size   uint64
	data   []Value
}

func NewColumn(hd string) Column {
	return &column{
		header: hd,
		size:   0,
		data:   []Value{},
	}
}

func (c *column) GetHeader() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.header
}

func (c *column) GetSize() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.size
}

func (c *column) GetValue(row uint64) (Value, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if row > c.size-1 {
		return nil, fmt.Errorf("index out of range (got %d for size %d)", row, c.size)
	}
	return c.data[row], nil
}

func (c *column) PushFront(v Value) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	temp := make([]Value, c.size+1)
	temp[0] = v
	copy(temp[1:], c.data)
	c.data = temp
	c.size++
	return c.size
}

func (c *column) PushBack(v Value) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data = append(c.data, v)
	c.size++
	return c.size
}

func (c *column) DeleteRow(row uint64) (Value, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if row > c.size-1 {
		return nil, fmt.Errorf("index out of range (got %d for size %d)", row, c.size)
	}
	v := c.data[row]
	copy(c.data[row:], c.data[row:])
	c.data = c.data[:len(c.data)-1 : len(c.data)-1]
	c.size--
	return v, nil
}

func (c *column) PopFront() (Value, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.size == 0 {
		return nil, false
	}
	v := c.data[0]
	c.data = c.data[1:len(c.data):len(c.data)]
	c.size--
	return v, true
}

func (c *column) PopBack() (Value, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.size == 0 {
		return nil, false
	}
	v := c.data[c.size-1]
	c.data = c.data[:len(c.data)-1 : len(c.data)-1]
	c.size--
	return v, true
}

// Frame contains data.
type Frame interface {
	AddColumn(c Column) error
	GetColumn(header string) (Column, error)
	GetColumnSize() int
	DeleteColumn(header string) bool
}

type frame struct {
	mu       sync.Mutex
	columns  []Column
	headerTo map[string]int
}

func NewFrame() Frame {
	return &frame{
		columns:  []Column{},
		headerTo: make(map[string]int),
	}
}

func (f *frame) AddColumn(c Column) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	header := c.GetHeader()
	if _, ok := f.headerTo[header]; ok {
		return fmt.Errorf("%q already exists", header)
	}
	f.columns = append(f.columns, c)
	f.headerTo[header] = len(f.columns) - 1
	return nil
}

func (f *frame) GetColumn(header string) (Column, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	idx, ok := f.headerTo[header]
	if !ok {
		return nil, fmt.Errorf("%q does not exist", header)
	}
	return f.columns[idx], nil
}

func (f *frame) GetColumnSize() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.columns)
}

func (f *frame) DeleteColumn(header string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	idx, ok := f.headerTo[header]
	if !ok {
		return false
	}
	if idx == 0 && len(f.headerTo) == 1 {
		f.headerTo = make(map[string]int)
		f.columns = []Column{}
		return true
	}

	copy(f.columns[idx:], f.columns[idx+1:])
	f.columns = f.columns[:len(f.columns)-1 : len(f.columns)-1]

	// update headerTo
	f.headerTo = make(map[string]int)
	for i, c := range f.columns {
		f.headerTo[c.GetHeader()] = i
	}
	return true
}
