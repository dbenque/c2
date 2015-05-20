package main

import (
	"flag"
	"log"
	"net"
	"net/http"
	"strconv"

	"github.com/dbenque/c2/coordinator"
	"github.com/dbenque/c2/endpointRegistry"
	"github.com/gorilla/mux"
)

var coordinatorID int
var port int
var cb_uri string
var cb_bucket string

func init() {
	const (
		portDefault      = 9000
		cb_bucketDefault = "default"
		cb_uriDefault    = "http://localhost:8091"
	)
	flag.IntVar(&port, "port", portDefault, "Define on which port to listen")
	flag.IntVar(&coordinatorID, "ID", 0, "Define the coordinator id")
	flag.StringVar(&cb_uri, "cb_pwd", cb_uriDefault, "Couchbase uri: http://host:8091/")
	flag.StringVar(&cb_bucket, "cb_bucket", cb_bucketDefault, "Couchbase bucket (default value is 'default')")
}

func main() {

	flag.Parse()

	if coordinatorID == 0 {
		log.Fatal("You must define a coordinator ID")
	}

	// initialize the coordinator resources (endpointRegistry, distributed datastore)
	if err := initResources(); err != nil {
		log.Fatal("Resource initialization failure: ", err)
	}

	// Register the handler to get the coordinator information
	r := mux.NewRouter()
	r.HandleFunc("/"+coordinator.CoordinatorInfoService, handleGetCoordinatorInfo)
	http.Handle("/", r)

	err := http.ListenAndServe(":"+strconv.Itoa(port), nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}

	done := make(chan bool)
	<-done
	return

}

var coordinatorInstance *coordinator.Coordinator

func initResources() error {

	registry, err := endpointRegistry.NewCouchbaseEndpointRegistry(cb_bucket, cb_uri)
	if err != nil {
		return err
	}

	coordinatorInstance, err = coordinator.NewCoordinator(coordinator.CoordinatorID(coordinatorID),
		nil,
		registry)
	coordinatorInstance.Start()

	coordinatorInstance.SetEndpoint(&endpointRegistry.Endpoint{net.TCPAddr{net.IPv4(127, 0, 0, 1), port, ""}})

	return nil
}

func handleGetCoordinatorInfo(w http.ResponseWriter, r *http.Request) {

	coordinatorInstance.GetCoordinatorInfo(w, r)

}
