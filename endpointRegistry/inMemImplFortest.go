package endpointRegistry

import (
	"sync"
	"fmt"
)

type wakeUpOnCount struct {
	c chan  int
	targetCount int
}

//MapEndpointRegistry this registry can be used for test purposes only
type MapEndpointRegistry struct {
	sync.Mutex
	endpoints map[string]Endpoint
	wakeUp * wakeUpOnCount
}

//NewMapEndpointRegistry create a new registry of type MapEndpointRegistry
func NewMapEndpointRegistry() *MapEndpointRegistry {

	return &MapEndpointRegistry{endpoints: make(map[string]Endpoint)}

}

//AddEndpoints implementation of AddEndpoints from interface EndpointRegistry
func (r *MapEndpointRegistry) AddEndpoints(endpoints []Endpoint) error {

	r.Lock()
	defer  r.Unlock();

	for _, v := range endpoints {
		r.endpoints[v.String()] = v
	}

	fmt.Println("Added:",len(r.endpoints))

	r.checkWakeUp()

	return nil
}

//RemoveEndpoints implementation of RemoveEndpoints from interface EndpointRegistry
func (r *MapEndpointRegistry) RemoveEndpoints(endpoints []Endpoint) error {

	r.Lock()
	defer  r.Unlock();

	for _, v := range endpoints {
		delete(r.endpoints, v.String())
	}

	fmt.Println("Removed:",len(r.endpoints))

 	r.checkWakeUp()

	return nil

}

//GetEndpoints implementation of GetEndpoints from interface EndpointRegistry
func (r *MapEndpointRegistry) GetEndpoints() ([]Endpoint, error) {

	r.Lock()
	defer  r.Unlock();

	var endpoints []Endpoint
	for _, v := range r.endpoints {
		endpoints = append(endpoints, v)
	}

	fmt.Println("Get:",len(r.endpoints))

	return endpoints, nil
}

//WakeUpOnCount convinient method for synchronization within test. Return a channel that will receive a count when the registry contains a specific number of entries.
func  (r *MapEndpointRegistry) WakeUpOnCount(target int) chan int{

	r.wakeUp = &wakeUpOnCount{make(chan int),target}
	return r.wakeUp.c
}

func (r*MapEndpointRegistry) checkWakeUp() {
 	// signal that the count match
 	if r.wakeUp!=nil && r.wakeUp.targetCount==len(r.endpoints) {
 		go func(){ r.wakeUp.c <- r.wakeUp.targetCount
			 fmt.Println("GIVING SIGNAL TO CHAN") }()		//Write in chan using goroutine to avoid blocking ... end thus deadlock
 	}

}
