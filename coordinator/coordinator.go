package coordinator

import (
	"fmt"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/dbenque/c2/distributedStore"
	"github.com/dbenque/c2/endpointRegistry"
)

const (
	CoordinatorInfoService      = "coordinator/Info" // URL for service giving coordinator info
	MaxFailureBeforeRemove      = 1                  // Number of failure before removing an endpoint
	SelfRegistrationInterval    = 2 * time.Second    // Interval for self registration of the endpoint
	RefreshClusterInterval      = 4 * time.Second    // Interval for refresh of the cluster
	no_handler_function_defined = "No handler function defined"
	unknown_ID_in_cluster       = "Unknown ID in the cluster"
)

type ID int
type coordinatorsIndex struct {
	sync.Mutex
	index map[ID]*Coordinator
}

func newCoordinatorsIndex() *coordinatorsIndex {
	return &coordinatorsIndex{index: make(map[ID]*Coordinator)}
}

type endpointsFailure struct {
	sync.Mutex
	count map[string]int // count of failure for a given endpoint (key = endpoint.String())
}

func newEndpointsFailure() *endpointsFailure {
	return &endpointsFailure{count: make(map[string]int)}
}

type requestHanlderFct func(w http.ResponseWriter, r *http.Request)

type Coordinator struct {
	store                distributedStore.DistributedStore // Resource to store task to be coordinated (couchbase, ETCD, redis, ...)
	registry             endpointRegistry.EndpointRegistry // Resource to register coordinator endpoint (couchbase, loadbalancer ...)
	ID                   ID                                // ID of the coordinator
	endpoint             *endpointRegistry.Endpoint        // Endpoint for this coordinator
	cluster              *coordinatorsIndex                // Cluster of coordinators ID->Coordinator
	failingEndpoint      *endpointsFailure                 // Endpoint Failures detection
	registrationTicker   *time.Ticker                      // Time to register again and again and again
	refreshClusterTicker *time.Ticker                      // Time to refresh cluster again and again and again
	requestHandler       requestHanlderFct                // Function that will handle the request
}

var instanceCoordinator *Coordinator

//InitCoordinator this create the Coordinator resource and launch all the process for automatic registration and cluster discovery (and cleaning)
func NewCoordinator(ID ID, store distributedStore.DistributedStore, registry endpointRegistry.EndpointRegistry) (*Coordinator, error) {

	c := Coordinator{
		store,
		registry,
		ID,
		nil,
		newCoordinatorsIndex(),
		newEndpointsFailure(),
		time.NewTicker(SelfRegistrationInterval),
		time.NewTicker(RefreshClusterInterval),
		nil,
	}

	// set the instance
	instanceCoordinator = &c
	return instanceCoordinator, nil
}

func (c *Coordinator) Start() {
	// Self Register frequently
	go func() {
		for _ = range c.registrationTicker.C {
			if c.endpoint == nil {
				return
			}
			c.registry.AddEndpoints([]endpointRegistry.Endpoint{*c.endpoint})
		}
	}()

	// Refresh cluster frequently (and clean failing endpoints)
	go func() {
		for _ = range c.refreshClusterTicker.C {
			if c.endpoint == nil {
				return
			}
			c.refreshCluster()
		}
	}()
}

func (c *Coordinator) Stop() {
	c.registrationTicker.Stop()
	c.refreshClusterTicker.Stop()
}

func (c *Coordinator) SetEndpoint(e *endpointRegistry.Endpoint) {
	c.endpoint = e
}

//Redirect search the coordinator endpoint assiociated to the ID and redirect the request
func (c *Coordinator) ProcessOrRedirect(anID ID, w http.ResponseWriter, r *http.Request) error {

	if anID == c.ID {
		if c.requestHandler != nil {
			(c.requestHandler)(w, r)
			w.Header().Add("c2_processing", strconv.Itoa(int(c.ID)))
			return nil
		} else {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(no_handler_function_defined))
			w.Header().Add("c2_processing", strconv.Itoa(int(c.ID)))
			return nil
		}
	}

	if targetCoordinator, ok := c.cluster.index[anID]; ok {
		fmt.Println("Forwarding to:"+targetCoordinator.endpoint.String())
		http.Redirect(w, r, "http://"+targetCoordinator.endpoint.String()+r.RequestURI, http.StatusFound)
		w.Header().Add("c2_forwardedBy", strconv.Itoa(int(c.ID)))
		return nil
	}

	w.WriteHeader(http.StatusInternalServerError)
	w.Write([]byte(unknown_ID_in_cluster))

	return errors.New(unknown_ID_in_cluster)

}

func (c *Coordinator) GetCoordinatorInfo(w http.ResponseWriter, r *http.Request) {

	if c == nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Header().Set("Content-Type", "text/plain")
		io.WriteString(w, "The coordinator was not initialized")
		return
	}

	// this is what is going to be read after in getCoordinatorInfoForEndPoint by other coordinator of the cluster
	// Pay attention to forward compatibility here when changing the Coordinator Struct
	buffer, err := json.Marshal(*c)

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Header().Set("Content-Type", "text/plain")
		io.WriteString(w, err.Error())
	}

	w.WriteHeader(http.StatusOK)
	w.Write(buffer)
}

// retrieve information of the coordinator associated to the endpoint. Mainly its ID to be able to forward task.
func getCoordinatorInfoForEndPoint(endPoint endpointRegistry.Endpoint) (*Coordinator, error) {

	res, err := http.Get("http://" + endPoint.IP.String() + ":" + strconv.Itoa(endPoint.Port) + "/" + CoordinatorInfoService)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	coordinatorInfo, err := ioutil.ReadAll(res.Body)
	res.Body.Close()

	c := new(Coordinator)
	err = json.Unmarshal(coordinatorInfo, &c)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (c *Coordinator) getClusterSize() int{
	if c.cluster==nil { return 0}

	return len(c.cluster.index)
}

func (c *Coordinator) refreshCluster() error {

	ePoints, err := c.registry.GetEndpoints()
	if err != nil {
		return err
	}

	newCluster := newCoordinatorsIndex()

	var wg sync.WaitGroup
	for _, ep := range ePoints {
		wg.Add(1)
		// poll endpoint in parallel
		go func(epoint endpointRegistry.Endpoint) {
			defer wg.Done()
			otherCoordinator, errep := getCoordinatorInfoForEndPoint(epoint)

			// protect the failure map from concurrent access since we play with multiple endpoints in parallel
			c.failingEndpoint.Lock()
			defer c.failingEndpoint.Unlock()

			if errep == nil {

				// set the correct endpoint (it was not part of the serialization, it is private)
				otherCoordinator.endpoint = &epoint

				// Ok we got the coordinator associated to the endpoint
				newCluster.Lock()
				newCluster.index[otherCoordinator.ID] = otherCoordinator
				newCluster.Unlock()

				// In case it was register as failing, clear because now it is fine
				delete(c.failingEndpoint.count, epoint.String())

			} else {

				// Bad new this endpoint is not responding!
				count, ok := c.failingEndpoint.count[epoint.String()]
				if ok {
					count++
				} else {
					count = 1
				}
				c.failingEndpoint.count[epoint.String()] = count

				// Check against max autorized failure before eviction
				if count > MaxFailureBeforeRemove {
					if err := c.registry.RemoveEndpoints([]endpointRegistry.Endpoint{epoint}); err == nil {
						delete(c.failingEndpoint.count, epoint.String())
						log.Println("Removing a failing endpoint: ", epoint.String())
					}
				}

			}
		}(ep)
	}
	wg.Wait()

	// Be carefull the cluster can be modified while you are working with it. Get the pointer to cluster and keep working with it till the end of your logic to be consistent
	c.cluster = newCluster

	return nil
}
