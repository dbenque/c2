package coordinator

import (
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/dbenque/c2/distributedStore"
	"github.com/dbenque/c2/endpointRegistry"

	"github.com/gorilla/mux"
)

const (
	testRegistrationTickerDuration   = 10 * time.Millisecond
	testRefreshClusterTickerDuration = 20 * time.Millisecond
	testCoordinatorCount             = 33
	testMagicStringForHandler = "MagicKeyForHandlerFct120478"
)

type coordinatorAndServer struct {
	server      *httptest.Server
	coordinator *Coordinator
}

type coordinatorsByEndpointForTest struct {
	sync.Mutex
	index map[string]coordinatorAndServer
}

// Start a new coordinator and its associated httptest.Server
func newTestCoordinator(id ID, store distributedStore.DistributedStore, registry endpointRegistry.EndpointRegistry, allCoordinators *coordinatorsByEndpointForTest) (*Coordinator,*httptest.Server) {
	c, _ := NewCoordinator(id, store, registry)

	c.registrationTicker = time.NewTicker(testRegistrationTickerDuration)
	c.refreshClusterTicker = time.NewTicker(testRefreshClusterTickerDuration)

	r := mux.NewRouter()
	// Handler to return the information about the coordinator
	r.HandleFunc("/"+CoordinatorInfoService, func(w http.ResponseWriter, r *http.Request) {
		c.GetCoordinatorInfo(w, r)
	})

	// Other handler simulating a task for a given coordinator ID
	r.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		id, _ := strconv.Atoi(r.FormValue("ID"))
		c.ProcessOrRedirect(ID(id), w, r)
	})

	// Launching the httptest.Server
	ts := httptest.NewServer(r)

	// Get the endpoint returned by the httptest.Server and associate it to the coordinator
	addr1, _ := net.ResolveTCPAddr("", ts.URL[7:])
	c.SetEndpoint(&endpointRegistry.Endpoint{*addr1})

	// Register the couple in the container used by the test
	allCoordinators.Lock()
	defer allCoordinators.Unlock()
	allCoordinators.index[c.endpoint.String()] = coordinatorAndServer{ts, c}

	return c,ts
}

// start a complete cluster
func startManyCoordinators(count int, allCoordinators *coordinatorsByEndpointForTest, store distributedStore.DistributedStore, registry endpointRegistry.EndpointRegistry) {
	// declare registrators
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(aID int) {
			defer wg.Done()
			c,_:=newTestCoordinator(ID(aID),
				store,
				registry,
				allCoordinators,
			)
			c.Start()

		}(i)
	}
	wg.Wait()

	// Wait a little bit
	time.Sleep(10 * testRegistrationTickerDuration)
}

func (csmap *coordinatorsByEndpointForTest) stopCoordiantorAndServer(endpointStr string) {
	csmap.Lock()
	defer csmap.Unlock()

	if cs, ok := csmap.index[endpointStr]; ok {
		delete(csmap.index, endpointStr)
		cs.coordinator.Stop()
		cs.server.Close()
	}
}

// empty the container and stop all the instances associated
func (csmap *coordinatorsByEndpointForTest) close() {

	for s, _ := range csmap.index {
		csmap.stopCoordiantorAndServer(s)
	}
}

// Test the fact that the Coordinator register correctly
func TestCoordinatorRegistration(t *testing.T) {

	// prepare test resources
	allCoordinators := coordinatorsByEndpointForTest{index: make(map[string]coordinatorAndServer)}
	defer allCoordinators.close()
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
	for k, _ := range allCoordinators.index {
		if _, ok := control[k]; !ok {
			t.Logf("The registry do not contains the following endpoint: %d", k)
			t.FailNow()
		}
	}
}

func TestCoordinatorRemovedWhenFailing(t *testing.T) {

	// prepare test resources
	allCoordinators := coordinatorsByEndpointForTest{index: make(map[string]coordinatorAndServer)}
	defer allCoordinators.close()
	registry := endpointRegistry.NewMapEndpointRegistry()
	startManyCoordinators(testCoordinatorCount, &allCoordinators, nil, registry)

	// Check how many record we have in the endpoint registry
	eps, _ := registry.GetEndpoints()
	if len(eps) != testCoordinatorCount {
		t.Logf("The registry do not contains all endpoints: %d/%d", len(eps), testCoordinatorCount)
		t.FailNow()
	}

	// Randomly kill half of them
	stoppedCoordinators := coordinatorsByEndpointForTest{index: make(map[string]coordinatorAndServer)}
	var wg sync.WaitGroup
	countRemove := 0
	for _, cAnds := range allCoordinators.index {
		wg.Add(1)
		go func(cs coordinatorAndServer) {

			defer wg.Done()

			// capture the instances that are stopped
			stoppedCoordinators.Lock()
			defer stoppedCoordinators.Unlock()
			epStr := cs.coordinator.endpoint.String()
			stoppedCoordinators.index[epStr] = cs

			allCoordinators.stopCoordiantorAndServer(epStr)

		}(cAnds)

		countRemove++
		if countRemove == testCoordinatorCount/2 {
			break
		}
	}

	wg.Wait()

	// Wait a little bit
	time.Sleep(10 * testRefreshClusterTickerDuration)

	// valide the number of coordinator remaining in the registry
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
	for k, cs := range stoppedCoordinators.index {
		if _, ok := control[k]; ok {
			t.Logf("The registry still contains the following endpoint associated to port: %d", cs.coordinator.endpoint.Port)
			t.FailNow()
		}
	}

}

func getBody(URL string) string {
	resp, _ := http.Get(URL)

	b, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	return string(b)

}

func TestForwarding(t *testing.T) {

	allCoordinators := coordinatorsByEndpointForTest{index: make(map[string]coordinatorAndServer)}
	registry := endpointRegistry.NewMapEndpointRegistry()
	defer allCoordinators.close()

	c1,s1:=newTestCoordinator(ID(1),nil,registry,&allCoordinators)
	defer s1.Close()
	c1.Start()

	c2,s2:=newTestCoordinator(ID(2),nil,registry,&allCoordinators)
	defer s2.Close()
	c2.Start()

	c2.requestHandler = func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(testMagicStringForHandler))
	}

	time.Sleep(10*time.Millisecond) // time for the server to start

	// Test that the output is correct when no forwarding and no function handler defined
	if getBody("http://"+c1.endpoint.String()+"/?ID=1")!=no_handler_function_defined {
		t.Fatalf("Should have returned: %s",no_handler_function_defined)
	}

	// Test that the output is correct when no forwarding and no function handler defined
	if getBody("http://"+c2.endpoint.String()+"/?ID=2")!=testMagicStringForHandler {
		t.Fatalf("C2 Should have returned: %s",testMagicStringForHandler)
	}

	// Test that the output is correct when no forwarding and no function handler defined
	if getBody("http://"+c1.endpoint.String()+"/?ID=9999")!=unknown_ID_in_cluster {
		t.Fatalf("Should have returned: %s",unknown_ID_in_cluster)
	}

	// Test that the output is correct when no forwarding and no function handler defined
	b:= getBody("http://"+c1.endpoint.String()+"/?ID=2")
	if b!=testMagicStringForHandler {
		t.Fatalf("C1 Should have returned: %s\n and not:%s",testMagicStringForHandler,b)
	}

	fmt.Println("%v",c1.cluster.index)


}
