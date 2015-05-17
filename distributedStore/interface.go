package distributedStore

import "time"

// Interface to be confirmed when design of Task Tree will come.

type DistributedStore interface {
	Lock(key string, ttl time.Duration) error
	Unlock(key string) error
	Set(key string, value string, ttl time.Duration) error
	Get(key string) (string, error)
}
