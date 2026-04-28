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

func StoreWithList(key string, values []string) *Store {
	store := NewStore()
	l := list.New()
	for _, el := range(values) {
		l.PushBack(el)
	}
	store.lists[key] = l
	return store
}

func (s *Store) SliceOfList(key string) []string {
	l, exists := s.lists[key]
	if !exists {
		return []string{}
	}
	result := []string{}
	for e := l.Front(); e != nil; e = e.Next() {
		result = append(result, e.Value.(string))
	}
	return result
}

func (s *Store) ListLen(key string) int {
	l, exists := s.lists[key]
	if !exists {
		return 0
	}
	return l.Len()
}

