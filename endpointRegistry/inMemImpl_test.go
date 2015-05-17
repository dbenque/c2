package endpointRegistry

import "sync"

type MapEndpointRegistry struct {
	sync.Mutex
	endpoints map[string]Endpoint
}

func NewMapEndpointRegistry() EndpointRegistry {

	return &MapEndpointRegistry{endpoints: make(map[string]Endpoint)}

}

func (r *MapEndpointRegistry) AddEndpoints(endpoints []Endpoint) error {

	r.Lock()
	defer r.Unlock()

	for _, v := range endpoints {
		r.endpoints[v.String()] = v
	}

	return nil
}

func (r *MapEndpointRegistry) RemoveEndpoints(endpoints []Endpoint) error {

	r.Lock()
	defer r.Unlock()

	for _, v := range endpoints {
		delete(r.endpoints, v.String())
	}

	return nil

}

func (r *MapEndpointRegistry) GetEndpoints() ([]Endpoint, error) {

	r.Lock()
	defer r.Unlock()

	var endpoints []Endpoint
	for _, v := range r.endpoints {
		endpoints = append(endpoints, v)
	}

	return endpoints, nil
}
