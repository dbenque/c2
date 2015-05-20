package coordinator

import (
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/dbenque/c2/distributedStore"
	"github.com/dbenque/c2/endpointRegistry"
)

const (
	testRegistrationTickerDuration   = 10 * time.Millisecond
	testRefreshClusterTickerDuration = 20 * time.Millisecond
	testCoordinatorCount             = 33
)

type coordinatorsByEndpointForTest struct {
	sync.Mutex
	index map[string]*Coordinator
}

func newTestCoordinator(ID CoordinatorID, endpoint endpointRegistry.Endpoint, store distributedStore.DistributedStore, registry endpointRegistry.EndpointRegistry, allCoordinators *coordinatorsByEndpointForTest) *Coordinator {
	c, _ := NewCoordinator(ID, store, registry)
	c.SetEndpoint(&endpoint)

	c.registrationTicker = time.NewTicker(testRegistrationTickerDuration)
	c.refreshClusterTicker = time.NewTicker(testRefreshClusterTickerDuration)

	allCoordinators.Lock()
	defer allCoordinators.Unlock()
	allCoordinators.index[endpoint.String()] = c

	// Function to mock the webservice call that normally get the coordinator info
	c.coordinatorInfoGetter = func(ep endpointRegistry.Endpoint) (*Coordinator, error) {
		allCoordinators.Lock()
		defer allCoordinators.Unlock()

		if otherCoordinator, ok := allCoordinators.index[ep.String()]; ok {
			return otherCoordinator, nil
		} else {
			return nil, errors.New("can't get info")
		}
	}

	return c
}

func startManyCoordinators(count int, allCoordinators *coordinatorsByEndpointForTest, store distributedStore.DistributedStore, registry endpointRegistry.EndpointRegistry) {
	// declare registrators
	var wg sync.WaitGroup
	for i := 9000; i < 9000+count; i++ {
		wg.Add(1)
		go func(ID int) {
			defer wg.Done()
			newTestCoordinator(CoordinatorID(ID),
				endpointRegistry.Endpoint{net.TCPAddr{net.IPv4(127, 0, 0, 1), ID, ""}},
				store,
				registry,
				allCoordinators,
			).Start()

		}(i)
	}
	wg.Wait()

	// Wait a little bit
	time.Sleep(10 * testRegistrationTickerDuration)
}

func TestCoordinatorRegistration(t *testing.T) {

	// prepare test resources
	allCoordinators := coordinatorsByEndpointForTest{index: make(map[string]*Coordinator)}
	registry := endpointRegistry.NewMapEndpointRegistry()

	startManyCoordinators(testCoordinatorCount, &allCoordinators, nil, registry)

	// Check how many record we have in the endpoint registry
	eps, _ := registry.GetEndpoints()
	if len(eps) != testCoordinatorCount {
		t.Logf("The registry do not contains all coordinators: %d/%d", len(eps), testCoordinatorCount)
		t.FailNow()
	}

	// Check that everybody is there
	control := make(map[string]bool)
	for _, ep := range eps {
		control[ep.String()] = true
	}

	for i := 9000; i < 9000+testCoordinatorCount; i++ {
		ep := endpointRegistry.Endpoint{net.TCPAddr{net.IPv4(127, 0, 0, 1), i, ""}}
		if _, ok := control[ep.String()]; !ok {
			t.Logf("The registry do not contains the following endpoint associated to port: %d", i)
			t.FailNow()
		}
	}
}

func TestCoordinatorRemovedWhenFailing(t *testing.T) {

	// prepare test resources
	allCoordinators := coordinatorsByEndpointForTest{index: make(map[string]*Coordinator)}
	registry := endpointRegistry.NewMapEndpointRegistry()
	startManyCoordinators(testCoordinatorCount, &allCoordinators, nil, registry)

	// Check how many record we have in the endpoint registry
	eps, _ := registry.GetEndpoints()
	if len(eps) != testCoordinatorCount {
		t.Logf("The registry do not contains all endpoints: %d/%d", len(eps), testCoordinatorCount)
		t.FailNow()
	}

	// Randomly kill half of them
	stoppedCoordinators := coordinatorsByEndpointForTest{index: make(map[string]*Coordinator)}
	var wg sync.WaitGroup
	countRemove := 0
	for _, c := range allCoordinators.index {
		wg.Add(1)
		go func(cc *Coordinator) {
			defer wg.Done()
			stoppedCoordinators.Lock()
			defer stoppedCoordinators.Unlock()
			stoppedCoordinators.index[cc.endpoint.String()] = cc
			allCoordinators.Lock()
			defer allCoordinators.Unlock()
			delete(allCoordinators.index, cc.endpoint.String())
			cc.Stop()

		}(c)

		countRemove++
		if countRemove == testCoordinatorCount/2 {
			break
		}
	}

	wg.Wait()

	// Wait a little bit
	time.Sleep(10 * testRefreshClusterTickerDuration)

	eps, _ = registry.GetEndpoints()
	if len(eps) != testCoordinatorCount-countRemove {
		t.Logf("The registry do not contains expected number of endpoints: %d/%d", len(eps), testCoordinatorCount-testCoordinatorCount/2)
		t.FailNow()
	}

	// Check that what was stopped is no more in the registry
	control := make(map[string]bool)
	for _, ep := range eps {
		control[ep.String()] = true
	}

	for k, c := range stoppedCoordinators.index {
		if _, ok := control[k]; ok {
			t.Logf("The registry still contains the following endpoint associated to port: %d", c.endpoint.Port)
			t.FailNow()
		}
	}

}
