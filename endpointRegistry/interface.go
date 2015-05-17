package endpointRegistry

import "net"

type Endpoint struct {
	net.TCPAddr
}

type EndpointRegistry interface {
	AddEndpoints(endpoints []Endpoint) error
	RemoveEndpoints(endpoints []Endpoint) error
	GetEndpoints() ([]Endpoint, error)
}
