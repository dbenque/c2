package task

type clusterID int

//ID type wrapping for identifier of task
type IDTask int

type IDCoordinatedWork int

type TaskState string

const (
	TaskStateNotDone TaskState = "NotDone"
	TaskStateDone    TaskState = "Done"
	TaskStateRunning TaskState = "Running"
)

//Task define a task that can be processed by a coordinator associated to a cluster
type Task struct {
	ID         IDTask
	ClusterID  clusterID
	RequestURI string
}

type TaskNode struct {
	Task
	TaskState string
	NextTasks []TaskNode
}

//CoordinatedWork tree structure of Task
type CoordinatedWork struct {
	ID       IDCoordinatedWork
	RootTask TaskNode
}
