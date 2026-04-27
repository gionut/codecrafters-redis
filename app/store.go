package main

import (
	"time"
	"container/list"
)

type Entry struct {
    Value  string
    Expiry time.Time // zero value means no expiry
}

func (e Entry) IsExpired() bool {
    return !e.Expiry.IsZero() && time.Now().After(e.Expiry)
}

type Store struct {
    strings map[string]Entry
    lists   map[string]*list.List
}

func NewStore() *Store {
    return &Store{
        strings: make(map[string]Entry),
        lists:   make(map[string]*list.List),
    }
}