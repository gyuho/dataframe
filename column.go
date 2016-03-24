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
