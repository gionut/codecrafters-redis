package main

import (
	"time"
	"container/list"
	"net"
	"fmt"
	"sync"
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
	requests map[string]*list.List
	mu sync.Mutex
}

type Request struct {
    conn    net.Conn
    timeout int
}

func NewStore() *Store {
    return &Store{
        strings: make(map[string]Entry),
        lists:   make(map[string]*list.List),
		requests: make(map[string]*list.List),
    }
}

// used for tests to hide underlying ds for store.lists
func StoreWithList(key string, values []string) *Store {
	store := NewStore()
	l := list.New()
	for _, el := range(values) {
		l.PushBack(el)
	}
	store.lists[key] = l
	return store
}

// used for tests
func EmptyRequestList() *list.List {
	return list.New()
}

// thread safety method to retrieve the number of elements within a list.
func (s *Store) ListLen(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	l, exists := s.lists[key]
	if !exists {
		return 0
	}
	return l.Len()
}

// Lpop element from list <key> if it is a non empty list, or register the blpop request otherwise to be served on rpush/lpush commands
func (s *Store) LPopOrRegister(key string, r Request) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	l, exists := s.lists[key]
    if exists && l.Len() > 0 {
        return l.Remove(l.Front()).(string), true
    }

    requests, exists := s.requests[key]
    if !exists {
        requests = EmptyRequestList()
        s.requests[key] = requests
    }
    requests.PushBack(r)
    return "", false
}


func (s *Store) notifyBlpopLocked(key string) bool {
	requests, exists := s.requests[key]
    if !exists || requests.Len() == 0 {
        return false
    }
	l, exists := s.lists[key]
	if !exists || l.Len() == 0 {
		return false
	}
    
	for requests.Len() > 0 && l.Len() > 0 {
		r := requests.Remove(requests.Front()).(Request)
		val := l.Remove(l.Front()).(string)
		r.conn.Write([]byte(fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(val), val)))
	}
    return true
}

// Pushes an element to the back of the list <key>. If the list does not exist, it will create it. 
// It also notifies blpop requests that new elements added to the list
func (s *Store) PushBack(key string, values []string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	l, exists := s.lists[key]
	if !exists {
		l = list.New()
		s.lists[key] = l
	}
	
	for _, el := range(values) {
		l.PushBack(el)
	}

	res := l.Len()
	s.notifyBlpopLocked(key)

	return res
}

// Pushes an element to the front of the list <key>. If the list does not exist, it will create it. 
// It also notifies blpop requests that there are new elements added to the list
func (s *Store) PushFront(key string, values []string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	l, exists := s.lists[key]
	if !exists {
		l = list.New()
		s.lists[key] = l
	}
	for _, el := range(values) {
		l.PushFront(el)
	}

	res := l.Len()
	s.notifyBlpopLocked(key)

	return res
}

func (s *Store) Lpop(key string, cnt int) []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	l, exists := s.lists[key]
	if !exists || l.Len() == 0 {
		return []string{}
	}

	result := []string{}
	for e := l.Front(); e != nil && cnt > 0; {
		next := e.Next()
		result = append(result, l.Remove(e).(string))
		cnt--
		e = next
	}
	return result
}

func (s *Store) SliceOfList(key string, start int, stop int) []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	l, exists := s.lists[key]
	if !exists || l.Len() == 0 {
		return []string{}
	}

	n := l.Len()
	if start < 0 {
		start = max(start+n, 0)
	}
	if stop < 0 {
		stop = stop + n
	}

	if start > stop || start >= n {
		return []string{}
	}

	stop = min(stop+1, n)
	result := []string{}

	i := 0
	for e := l.Front(); e != nil; e = e.Next() {
		if i >= stop {
			break
		}
		if i >= start {
			result = append(result, e.Value.(string))
		}
		i++
	}
	return result
}