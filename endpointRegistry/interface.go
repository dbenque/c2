package endpointRegistry

import (
	"encoding/json"
	"log"
	"net"

	"github.com/dbenque/c2/distributedStore/couchbase"
)

type Endpoint struct {
	net.TCPAddr
}

type EndpointRegistry interface {
	AddEndpoints(endpoints []Endpoint) error
	RemoveEndpoints(endpoints []Endpoint) error
	GetEndpoints() ([]Endpoint, error)
}

// -----------------------------------------------------
// -----------------------------------------------------
// ----Couchbase flavor of the endpointregistry---------
// -----------------------------------------------------
// -----------------------------------------------------

type CouchbaseEndpointRegistry struct {
	couchbaseStore.CBConnection
}

const (
	endpointListName = "EndpointRegistry"
)

func NewCouchbaseEndpointRegistry(cb_bucket string, cb_uri string) (EndpointRegistry, error) {
	cb, err := couchbaseStore.GetCBConnection(cb_bucket, cb_uri)
	if err != nil {
		return nil, err
	}
	return &CouchbaseEndpointRegistry{*cb}, nil
}

func (cbr *CouchbaseEndpointRegistry) AddEndpoints(endpoints []Endpoint) error {

	var errMain error
	errMain = nil
	for _, v := range endpoints {
		if dataj, err := json.Marshal(v); err != nil {
			errMain = err
		} else {
			if err := cbr.AddUniqueInStrList(endpointListName, string(dataj)); err != nil {
				errMain = err
			}
		}
	}
	return errMain

}

func (cbr *CouchbaseEndpointRegistry) RemoveEndpoints(endpoints []Endpoint) error {
	var errMain error

	for _, v := range endpoints {
		if dataj, err := json.Marshal(v); err != nil {
			errMain = err
		} else {
			if err := cbr.RemoveFromStrList(endpointListName, string(dataj)); err != nil {
				errMain = err
			}
		}
	}
	return errMain
}

func (cbr *CouchbaseEndpointRegistry) GetEndpoints() ([]Endpoint, error) {
	s, err := cbr.GetStrList(endpointListName)
	if err != nil {
		return nil, err
	}

	var endpoints []Endpoint
	for _, v := range s {
		var e Endpoint
		if err := json.Unmarshal([]byte(v), &e); err == nil {
			endpoints = append(endpoints, e)
		} else {
			log.Println("Error while unserializing endpoint")
		}
	}

	return endpoints, nil
}
