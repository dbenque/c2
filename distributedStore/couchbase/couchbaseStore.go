package couchbaseStore

import (
	"fmt"

	"github.com/couchbase/go-couchbase"
)

type CBConnection struct {
	client *couchbase.Client
	pool   *couchbase.Pool
	bucket *couchbase.Bucket
}

//GetCBConnection initialize the connection with couchbase.
func GetCBConnection(cb_bucket string, cb_uri string) (*CBConnection, error) {
	// Init distributed store
	c, err := couchbase.Connect(cb_uri)
	if err != nil {
		return nil, err
	}

	p, err := c.GetPool("default")
	if err != nil {
		return nil, err
	}

	b, err := p.GetBucket(cb_bucket)
	if err != nil {
		return nil, err
	}

	err = b.Set("test_connection", 1, map[string]interface{}{"test": 1})
	if err != nil {
		return nil, err
	}

	return &CBConnection{&c, &p, b}, nil
}

//AddUniqueInStrList this will be used when couchbase is used as endpoint registry
func (cb *CBConnection) AddUniqueInStrList(listName string, value string) error {

	c := make(chan bool)
	done := false
	retryCount := 0
	for !done && retryCount < 100 {
		go func() {
			var listStr []string
			var cas uint64
			if err := cb.bucket.Gets(listName, &listStr, &cas); err != nil {
				// TODO test that it is a error type miss and nothing else

				// create the list
				cb.bucket.Set(listName, 0, []string{value})
				c <- true
				return
			}

			for _, v := range listStr {
				if v == value {
					c <- true
					return
				} // value already present in the list
			}

			listStr = append(listStr, value)
			if err := cb.bucket.Cas(listName, 0, cas, &listStr); err == nil {
				c <- true
				return
			}
			c <- false

		}()
		done = <-c
		retryCount++

	}

	if retryCount >= 100 {
		return fmt.Errorf("Could not add element '%s' to list '%s'", value, listName)
	}

	return nil
}

//GetStrList this will be used when couchbase is used as endpoint registry
func (cb *CBConnection) GetStrList(listName string) ([]string, error) {
	var listStr []string

	cb.bucket.Get(listName, &listStr)
	// TODO test that it is a error type miss and nothing else

	return listStr, nil

}

//RemoveFromStrList this will be used when couchbase is used as endpoint registry
func (cb *CBConnection) RemoveFromStrList(listName string, value string) error {

	c := make(chan bool)
	done := false
	retryCount := 0
	for !done && retryCount < 100 {
		go func() {
			var listStr []string
			var cas uint64
			if err := cb.bucket.Gets(listName, &listStr, &cas); err != nil {
				// TODO test that it is a error type miss and nothing else
				// empty list, nothing to remove
				c <- true
				return
			}

			for i, v := range listStr {
				if v == value {
					listStr = append(listStr[:i], listStr[i+1:]...)
					if err := cb.bucket.Cas(listName, 0, cas, &listStr); err == nil {
						c <- true
						return
					}
					c <- false
					return
				}
			}

			// value not in the list
			c <- true
			return

		}()
		done = <-c
		retryCount++

	}

	if retryCount >= 100 {
		return fmt.Errorf("Could not remove element '%s' to list '%s'", value, listName)
	}

	return nil

}
