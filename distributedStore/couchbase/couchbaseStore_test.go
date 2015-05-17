package couchbaseStore

import (
	"strconv"
	"sync"
	"testing"
)

const (
	newList   = "test_newList"
	newValue  = "new"
	dupeValue = "dupe"
)

// TODO : a dedicated bucket should be created on the fly and dedicated to that test. Else concurrent testing could fail.

func clearTestData(t *testing.T) {
	cb, err := GetCBConnection("default", "http://localhost:8091")
	if err != nil {
		t.Fatalf("Clear: Can't establish Couchbase connection: %s", err.Error())
	}

	cb.bucket.Delete(newList)
}

func TestAddUniqueInStrList(t *testing.T) {

	clearTestData(t)

	cb, err := GetCBConnection("default", "http://localhost:8091")
	if err != nil {
		t.Fatalf("Can't establish Couchbase connection: %s", err.Error())
	}

	// Test empty list
	l, _ := cb.GetStrList(newList)
	if len(l) != 0 {
		t.Fatalf("The test should retrieve an empty list")
	}

	//check creation of a list
	cb.AddUniqueInStrList(newList, newValue)

	l, _ = cb.GetStrList(newList)
	if len(l) != 1 {
		t.Fatalf("The test should retrieve a list with 1 elem")
	}

	//check adding
	cb.AddUniqueInStrList(newList, dupeValue)
	l, _ = cb.GetStrList(newList)
	if len(l) != 2 {
		t.Fatalf("The test should retrieve a list with 2 elem")
	}

	//test no dupe
	cb.AddUniqueInStrList(newList, dupeValue)
	l, _ = cb.GetStrList(newList)
	if len(l) != 2 {
		t.Fatalf("The test should retrieve a list with 2 elem")
	}

	//test remove
	cb.RemoveFromStrList(newList, dupeValue)
	l, _ = cb.GetStrList(newList)
	if len(l) != 1 {
		t.Fatalf("The test should retrieve a list with 1 elem")
	}

	//test remove unknown value
	cb.RemoveFromStrList(newList, dupeValue)
	l, _ = cb.GetStrList(newList)
	if len(l) != 1 {
		t.Fatalf("The test should retrieve a list with 1 elem (remove unknown)")
	}

	//test remove last value
	cb.RemoveFromStrList(newList, newValue)
	l, _ = cb.GetStrList(newList)
	if len(l) != 0 {
		t.Fatalf("The test should retrieve a list with 0 elem (remove last)")
	}

	// concurrency test
	var wg sync.WaitGroup

	//test concurrency add
	count := 100
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(val int) {
			cb.AddUniqueInStrList(newList, strconv.Itoa(val))
			wg.Done()
		}(i)
	}
	wg.Wait()

	l, _ = cb.GetStrList(newList)
	if len(l) != count {
		t.Fatalf("The test should retrieve a list with %d elem", count)
	}

	//test concurrency remove
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(val int) {
			cb.RemoveFromStrList(newList, strconv.Itoa(val))
			wg.Done()
		}(i)
	}
	wg.Wait()

	l, _ = cb.GetStrList(newList)
	if len(l) != 0 {
		t.Fatalf("The test should retrieve a list with %d elem", 0)
	}

	//clearTestData(t)
}
