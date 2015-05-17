package coordinator

import (
	"encoding/json"
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
	CoordinatorInfoService   = "coordinator/Info" // URL for service giving coordinator info
	MaxFailureBeforeRemove   = 1                  // Number of failure before removing an endpoint
	SelfRegistrationInterval = 2 * time.Second    // Interval for self registration of the endpoint
	RefreshClusterInterval   = 4 * time.Second    // Interval for refresh of the cluster
)

type coordinators map[int]Coordinator

type Coordinator struct {
	store           distributedStore.DistributedStore // Resource to store task to be coordinated (couchbase, ETCD, redis, ...)
	registry        endpointRegistry.EndpointRegistry // Resource to register coordinator endpoint (couchbase, loadbalancer ...)
	ID              int                               // ID of the coordinator
	endpoint        endpointRegistry.Endpoint         // Endpoint for this coordinator
	cluster         coordinators                      // Cluster of coordinators ID->Coordinator
	failingEndpoint map[string]int                    // Endpoint Failures detection
}

var instanceCoordinator *Coordinator

//InitCoordinator this create the Coordinator resource and launch all the process for automatic registration and cluster discovery (and cleaning)
func InitCoordinator(ID int, endpoint endpointRegistry.Endpoint, store distributedStore.DistributedStore, registry endpointRegistry.EndpointRegistry) (*Coordinator, error) {

	instanceCoordinator = &Coordinator{
		store,
		registry,
		ID,
		endpoint,
		make(coordinators),
		make(map[string]int)}

	// Self Register frequently
	go func() {
		c := time.Tick(SelfRegistrationInterval)
		for range c {
			if instanceCoordinator != nil && instanceCoordinator.registry != nil {
				var listEndpoint []endpointRegistry.Endpoint
				listEndpoint = append(listEndpoint, instanceCoordinator.endpoint)
				instanceCoordinator.registry.AddEndpoints(listEndpoint)
			}
		}
	}()

	// Refresh cluster frequently (and clean failing endpoints)
	go func() {
		c := time.Tick(RefreshClusterInterval)
		for range c {
			if instanceCoordinator != nil && instanceCoordinator.registry != nil {
				instanceCoordinator.refreshCluster()
			}
		}
	}()

	return instanceCoordinator, nil
}

func (c *Coordinator) refreshCluster() error {

	ePoints, err := c.registry.GetEndpoints()
	if err != nil {
		return err
	}

	newCluster := make(coordinators)

	var mutex = &sync.Mutex{}

	var wg sync.WaitGroup
	for _, ep := range ePoints {
		wg.Add(1)
		// poll endpoint in parallel
		go func(epoint endpointRegistry.Endpoint) {
			defer wg.Done()
			if otherCoordinator, errep := getCoordinatorInfoForEndPoint(epoint); errep == nil {
				mutex.Lock()
				newCluster[otherCoordinator.ID] = *otherCoordinator
				mutex.Unlock()

				// TODO I should mutex the failingEndpoint map !!!
				delete(c.failingEndpoint, epoint.String())

			} else {
				if count, ok := c.failingEndpoint[epoint.String()]; ok {
					count++

					// TODO I should mutex the failingEndpoint map !!!
					c.failingEndpoint[epoint.String()] = count
					if count > MaxFailureBeforeRemove {
						var epl []endpointRegistry.Endpoint
						if err := c.registry.RemoveEndpoints(append(epl, epoint)); err == nil {
							delete(c.failingEndpoint, epoint.String())
							log.Println("Removing a failing endpoint: ", epoint.String())
						}

					}
				} else {
					// add as failing endpoint
					c.failingEndpoint[epoint.String()] = 1
				}
			}
		}(ep)
	}
	wg.Wait()

	// TODO I should mutex the cluster map !!!
	c.cluster = newCluster

	return nil
}

// HandleGetCoordinatorInfo handler to register to the webserver associated to the coordinator. This returns the information of the coordinator.
func HandleGetCoordinatorInfo(w http.ResponseWriter, r *http.Request) {

	if instanceCoordinator == nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Header().Set("Content-Type", "text/plain")
		io.WriteString(w, "The coordinator was not initialized")
		return
	}

	// this is what is going to be read after in getCoordinatorInfoForEndPoint by other coordinator of the cluster
	// Pay attention to forward compatibility here when changing the Coordinator Struct
	buffer, err := json.Marshal(*instanceCoordinator)

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
