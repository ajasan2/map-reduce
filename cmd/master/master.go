package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"mapreduce/cmd/database"
	pb "mapreduce/protos"

	"github.com/golang/glog"
	"github.com/gorilla/mux"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

const (
	mapTimeout    = 60 * time.Second
	reduceTimeout = 600 * time.Second
	checkInterval = 5 * time.Second
)

type JobConfig struct {
	NumMappers     int    `json:"num_mappers"`
	NumReducers    int    `json:"num_reducers"`
	InputContainer string `json:"input_container"`
}

type MasterState struct {
	MapTasks    []*pb.Shard
	ReduceTasks []*pb.ReduceRequest
	Phase       string // "map" or "reduce"
}

type MMServer struct {
	pb.UnimplementedMasterMasterServer
}

type EtcdManager struct {
	client *clientv3.Client
	mu     sync.RWMutex
}

var (
	etcdManager *EtcdManager
	once        sync.Once
)

var (
	activeServer    *http.Server
	serverMutex     sync.Mutex
	masterState     *MasterState
	masterStateLock sync.Mutex
	otherMasters    []string
	workerAddrs     []string
	port            = flag.Int("port", 50052, "The gRPC server port")
	taskCounter     = 0        // Global counter for all tasks
	failAfter       = 0        // Global parameter for controlled failure
	taskCounterLock sync.Mutex // Lock for the task counter
)

// Helper function for atomic state updates with backup
func updateMasterStateAndBackup(update func()) {
	masterStateLock.Lock()
	update()
	masterStateLock.Unlock()

	// Backup state after any modification
	backupStateToFollowers()
}

func backupStateToFollowers() {
	glog.V(2).Info("Pushing masterState to all followers")

	masterStateLock.Lock()
	state := &pb.State{
		MapTasks:    masterState.MapTasks,
		ReduceTasks: masterState.ReduceTasks,
		Phase:       masterState.Phase,
	}
	masterStateLock.Unlock()

	glog.V(3).Infof("Current state being backed up - Phase: %s, Map Tasks: %d, Reduce Tasks: %d",
		state.Phase, len(state.MapTasks), len(state.ReduceTasks))

	for _, followerAddr := range otherMasters {
		glog.V(3).Infof("Attempting to backup state to follower at %s", followerAddr)

		conn, err := grpc.Dial(followerAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
			grpc.WithTimeout(5*time.Second))
		if err != nil {
			glog.Errorf("Failed to connect to follower %s: %v", followerAddr, err)
			continue
		}

		client := pb.NewMasterMasterClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		resp, err := client.PushState(ctx, state)
		cancel()
		conn.Close()

		if err != nil {
			glog.Errorf("Failed to backup state to follower %s: %v", followerAddr, err)
		} else {
			glog.V(2).Infof("Successfully backed up state to follower %s: %s", followerAddr, resp.Outcome)
		}
	}
}

// Function to handle receiving state updates from leader
func (s *MMServer) PushState(ctx context.Context, inState *pb.State) (*pb.Response, error) {
	glog.V(2).Infof("Received state backup from leader")

	masterStateLock.Lock()
	defer masterStateLock.Unlock()

	// Initialize masterState if it doesn't exist
	if masterState == nil {
		masterState = &MasterState{
			MapTasks:    []*pb.Shard{},
			ReduceTasks: []*pb.ReduceRequest{},
			Phase:       "map",
		}
	}

	glog.V(3).Infof("Current state before update - Phase: %s, Map Tasks: %d, Reduce Tasks: %d",
		masterState.Phase, len(masterState.MapTasks), len(masterState.ReduceTasks))
	glog.V(3).Infof("Incoming state - Phase: %s, Map Tasks: %d, Reduce Tasks: %d",
		inState.Phase, len(inState.MapTasks), len(inState.ReduceTasks))

	masterState.MapTasks = inState.MapTasks
	masterState.ReduceTasks = inState.ReduceTasks
	masterState.Phase = inState.Phase

	glog.V(2).Infof("Updated state - Phase: %s, Map Tasks: %d, Reduce Tasks: %d",
		masterState.Phase, len(masterState.MapTasks), len(masterState.ReduceTasks))

	return &pb.Response{
		Outcome: "SUCCESS",
	}, nil
}

// Function to execute tasks on workers
func executeTask(task interface{}, workerAddr string) error {
	conn, err := grpc.Dial(workerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("did not connect to worker at %s: %v", workerAddr, err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	c := pb.NewMasterWorkerClient(conn)

	var resp *pb.Response
	switch t := task.(type) {
	case *pb.Shard:
		glog.V(2).Infof("Sending Map GRPC request for shard %d to worker %s", t.ShardId, workerAddr)
		resp, err = c.ExecuteMap(ctx, t)
	case *pb.ReduceRequest:
		glog.V(2).Infof("Sending Reduce GRPC request for reducer %d to worker %s", t.ReducerId, workerAddr)
		resp, err = c.ExecuteReduce(ctx, t)
	default:
		return fmt.Errorf("unknown task type")
	}

	if err != nil {
		return err
	}

	glog.V(2).Infof("Response from worker: %s", resp.Outcome)
	return nil
}

// Function to monitor task timeouts
func monitorTimeouts(done chan bool) {
	manager, err := GetEtcdManager()
	if err != nil {
		glog.Errorf("Failed to get etcd manager: %v", err)
		return
	}

	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			now := time.Now().UnixNano()

			// Print current system state
			printTaskStatus()

			// Check timeouts and handle failed tasks
			for _, addr := range workerAddrs {
				worker, err := manager.GetWorkerState(addr)
				if err != nil {
					continue
				}

				if worker.Task != nil && worker.StartExecution != 0 {
					taskTimeout := mapTimeout
					if _, isReduce := worker.Task.(*pb.Worker_Reduce); isReduce {
						taskTimeout = reduceTimeout
					}

					execTime := time.Duration(now - worker.StartExecution)
					if execTime > taskTimeout {
						// Update master state and backup on timeout
						updateMasterStateAndBackup(func() {
							if mapTask, isMap := worker.Task.(*pb.Worker_Map); isMap {
								glog.Warningf("Map task (Shard %d) on worker %s timed out (%v)",
									mapTask.Map.ShardId, addr, execTime.Round(time.Second))
								masterState.MapTasks = append(masterState.MapTasks, mapTask.Map)
							} else if reduceTask, isReduce := worker.Task.(*pb.Worker_Reduce); isReduce {
								glog.Warningf("Reduce task (Reducer %d) on worker %s timed out (%v)",
									reduceTask.Reduce.ReducerId, addr, execTime.Round(time.Second))
								masterState.ReduceTasks = append(masterState.ReduceTasks, reduceTask.Reduce)
							}
						})

						err := updateWorkerWithRetry(addr, func(w *pb.Worker) error {
							w.Task = nil
							w.StartExecution = 0
							return nil
						})
						if err != nil {
							glog.Errorf("Failed to clear worker state: %v", err)
						}
					}
				}
			}

			// Try to assign pending tasks
			assignPendingTasks()
		}
	}
}

func getReachableIdleWorkers() ([]string, error) {
	manager, err := GetEtcdManager()
	if err != nil {
		return nil, fmt.Errorf("failed to get etcd manager: %v", err)
	}

	var availableWorkers []string
	for _, addr := range workerAddrs {
		// Check if worker has a task in etcd
		worker, err := manager.GetWorkerState(addr)
		if err != nil && err.Error() != "worker not found" {
			glog.Errorf("Error checking worker state for %s: %v", addr, err)
			continue
		}

		// Only consider workers that are either not in etcd or have no task
		if (err != nil && err.Error() == "worker not found") || (worker != nil && worker.Task == nil) {
			// Verify worker is reachable
			conn, err := grpc.Dial(addr,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithBlock(),
				grpc.WithTimeout(2*time.Second))
			if err != nil {
				glog.V(2).Infof("Worker %s is unreachable: %v", addr, err)
				continue
			}
			conn.Close()
			availableWorkers = append(availableWorkers, addr)
		}
	}

	return availableWorkers, nil
}

func assignPendingTasks() {
	manager, err := GetEtcdManager()
	if err != nil {
		glog.Errorf("Failed to get etcd manager: %v", err)
		return
	}

	masterStateLock.Lock()
	currentPhase := masterState.Phase
	hasMapTasks := len(masterState.MapTasks) > 0
	hasReduceTasks := len(masterState.ReduceTasks) > 0
	masterStateLock.Unlock()

	// Get reachable idle workers
	availableWorkers, err := getReachableIdleWorkers()
	if err != nil {
		glog.Errorf("Failed to get reachable workers: %v", err)
		return
	}

	activeMapTasks := false
	// Check for active map tasks before starting reduce phase
	for _, addr := range workerAddrs {
		worker, err := manager.GetWorkerState(addr)
		if err != nil {
			continue
		}
		if worker.Task != nil {
			if _, isMap := worker.Task.(*pb.Worker_Map); isMap {
				activeMapTasks = true
				break
			}
		}
	}

	if len(availableWorkers) > 0 {
		switch currentPhase {
		case "map":
			if hasMapTasks {
				glog.V(2).Infof("Found %d available workers for pending map tasks", len(availableWorkers))
				for _, addr := range availableWorkers {
					var task *pb.Shard
					masterStateLock.Lock()
					if len(masterState.MapTasks) > 0 {
						task = masterState.MapTasks[0]
						masterState.MapTasks = masterState.MapTasks[1:]
					}
					masterStateLock.Unlock()

					if task != nil {
						// Update worker state in etcd
						err := updateWorkerWithRetry(addr, func(w *pb.Worker) error {
							w.Task = &pb.Worker_Map{Map: task}
							w.StartExecution = time.Now().UnixNano()
							return nil
						})
						if err != nil {
							masterStateLock.Lock()
							masterState.MapTasks = append(masterState.MapTasks, task)
							masterStateLock.Unlock()
							glog.Errorf("Failed to assign map task to worker %s: %v", addr, err)
							continue
						}

						// Backup state after successful assignment
						backupStateToFollowers()

						glog.V(2).Infof("Assigned Map task (Shard %d) to worker %s", task.ShardId, addr)

						// Execute task synchronously
						if err := executeTask(task, addr); err != nil {
							glog.Error(err)
							masterStateLock.Lock()
							masterState.MapTasks = append(masterState.MapTasks, task)
							masterStateLock.Unlock()

							updateWorkerWithRetry(addr, func(w *pb.Worker) error {
								w.Task = nil
								w.StartExecution = 0
								return nil
							})

							backupStateToFollowers()
							continue
						}

						taskCounterLock.Lock()
						taskCounter++
						currentCount := taskCounter
						shouldFail := failAfter > 0 && currentCount >= failAfter
						taskCounterLock.Unlock()

						if shouldFail {
							glog.V(1).Infof("FAILED (intentionally) after assigning task - count: %d", currentCount)
							os.Exit(0)
							return
						}
					}
				}
			}

		case "reduce":
			if hasReduceTasks && !activeMapTasks {
				glog.V(2).Infof("Found %d available workers for pending reduce tasks", len(availableWorkers))
				for _, addr := range availableWorkers {
					var task *pb.ReduceRequest
					masterStateLock.Lock()
					if len(masterState.ReduceTasks) > 0 {
						task = masterState.ReduceTasks[0]
						masterState.ReduceTasks = masterState.ReduceTasks[1:]
					}
					masterStateLock.Unlock()

					if task != nil {
						// Update worker state in etcd
						err := updateWorkerWithRetry(addr, func(w *pb.Worker) error {
							w.Task = &pb.Worker_Reduce{Reduce: task}
							w.StartExecution = time.Now().UnixNano()
							return nil
						})
						if err != nil {
							masterStateLock.Lock()
							masterState.ReduceTasks = append(masterState.ReduceTasks, task)
							masterStateLock.Unlock()
							glog.Errorf("Failed to assign reduce task to worker %s: %v", addr, err)
							continue
						}

						backupStateToFollowers()

						glog.V(2).Infof("Assigned Reduce task (Reducer %d) to worker %s", task.ReducerId, addr)

						if err := executeTask(task, addr); err != nil {
							glog.Error(err)
							masterStateLock.Lock()
							masterState.ReduceTasks = append(masterState.ReduceTasks, task)
							masterStateLock.Unlock()

							updateWorkerWithRetry(addr, func(w *pb.Worker) error {
								w.Task = nil
								w.StartExecution = 0
								return nil
							})

							backupStateToFollowers()
							continue
						}

						taskCounterLock.Lock()
						taskCounter++
						currentCount := taskCounter
						shouldFail := failAfter > 0 && currentCount >= failAfter
						taskCounterLock.Unlock()

						if shouldFail {
							glog.V(1).Infof("FAILED (intentionally) after assigning task - count: %d", currentCount)
							os.Exit(0)
							return
						}
					}
				}
			}
		}
	}
}

func handleJobSubmission(w http.ResponseWriter, r *http.Request) {
	var config JobConfig
	if err := json.NewDecoder(r.Body).Decode(&config); err != nil {
		http.Error(w, fmt.Sprintf("Invalid job configuration: %v", err), http.StatusBadRequest)
		return
	}

	glog.V(2).Infof("Received job request: %d Mapper Tasks, %d Reducer Tasks", config.NumMappers, config.NumReducers)

	// Send acceptance response immediately
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{
		"status": "Job submitted successfully",
	})

	// Execute job asynchronously
	go func() {
		shards, err := shardFiles(config.NumMappers, config.NumReducers, config.InputContainer)
		if err != nil {
			glog.Errorf("Failed to shard files: %v", err)
			return
		}

		if err := executeMapReduceTasks(shards, config.NumReducers); err != nil {
			glog.Errorf("Failed to execute MapReduce tasks: %v", err)
			return
		}

		glog.Infof("Job completed successfully")
	}()
}

func setupStorage(numReducers int) error {
	db, err := database.GetStorage(context.Background(), os.Getenv("AZURE_STORAGE_ACCOUNT"))
	if err != nil {
		return err
	}

	err = db.CreateNewContainer(context.Background(), "intermediate")
	if err != nil {
		glog.Errorf("Error creating intermediate container: %v", err)
		return err
	}

	for i := 1; i <= numReducers; i++ {
		intermediateFileName := fmt.Sprintf("intermediate_%d.txt", i)
		err := db.CreateAppendBlob(context.Background(), "intermediate", intermediateFileName)
		if err != nil {
			glog.Errorf("Error creating intermediate append blob %s: %v", intermediateFileName, err)
			return err
		}
	}
	return nil
}

func printTaskStatus() {
	manager, err := GetEtcdManager()
	if err != nil {
		glog.Errorf("Failed to get etcd manager: %v", err)
		return
	}

	glog.Info("========== Current Task Status ==========")

	// Print master state
	masterStateLock.Lock()
	if masterState != nil {
		glog.Infof("Current Phase: %s", masterState.Phase)
		glog.Infof("Pending Map Tasks: %d", len(masterState.MapTasks))
		for _, task := range masterState.MapTasks {
			glog.Infof("  - Shard %d (Files: %d)", task.ShardId, len(task.Files))
		}
		glog.Infof("Pending Reduce Tasks: %d", len(masterState.ReduceTasks))
		for _, task := range masterState.ReduceTasks {
			glog.Infof("  - Reducer %d", task.ReducerId)
		}
	}
	masterStateLock.Unlock()

	// Print worker states
	glog.Info("Worker Status:")
	for _, addr := range workerAddrs {
		worker, err := manager.GetWorkerState(addr)
		if err != nil {
			if err.Error() == "worker not found" {
				glog.Infof("Worker %s: Available (No state in etcd)", addr)
			} else {
				glog.Errorf("Error getting worker %s state: %v", addr, err)
			}
			continue
		}

		status := "Idle"
		taskInfo := ""
		execTime := "N/A"

		if worker.Task != nil {
			if mapTask, isMap := worker.Task.(*pb.Worker_Map); isMap {
				status = "Running Map"
				taskInfo = fmt.Sprintf("Shard %d", mapTask.Map.ShardId)
			} else if reduceTask, isReduce := worker.Task.(*pb.Worker_Reduce); isReduce {
				status = "Running Reduce"
				taskInfo = fmt.Sprintf("Reducer %d", reduceTask.Reduce.ReducerId)
			}

			if worker.StartExecution != 0 {
				execTime = time.Duration(time.Now().UnixNano() - worker.StartExecution).Round(time.Second).String()
			}
		}

		glog.Infof("Worker %s: %s %s (Running time: %s)", addr, status, taskInfo, execTime)
	}

	glog.Info("=========================================")
}
func executeMapReduceTasks(shards []*pb.Shard, numReducers int) error {
	var err error

	// Initialize fail parameter from environment variable
	failStr := os.Getenv("FAIL")
	if failStr != "" {
		failAfter, err = strconv.Atoi(failStr)
		if err != nil {
			glog.Errorf("Invalid FAIL parameter: %v, defaulting to stable master", err)
			failAfter = 0
		}

		if failAfter == 0 {
			glog.V(1).Info("Running as stable master (will not fail)")
		} else {
			glog.V(1).Infof("Master will fail after processing %d tasks", failAfter)
		}
	} else {
		glog.V(1).Info("No FAIL parameter set, running as stable master")
		failAfter = 0
	}

	// Reset task counter
	taskCounter = 0

	// Initialize workers and get other master addresses
	workerAddrs, err = initializeWorkers()
	if err != nil {
		return fmt.Errorf("failed to get worker addresses: %v", err)
	}

	otherMasters, err = getMasterAddresses()
	if err != nil {
		return fmt.Errorf("failed to get master addresses: %v", err)
	}

	// Initialize master state with backup
	updateMasterStateAndBackup(func() {
		masterState = &MasterState{
			MapTasks:    shards,
			ReduceTasks: make([]*pb.ReduceRequest, numReducers),
			Phase:       "map",
		}
		for i := 0; i < numReducers; i++ {
			masterState.ReduceTasks[i] = &pb.ReduceRequest{
				ReducerId: int64(i + 1),
			}
		}
	})

	if err := setupStorage(numReducers); err != nil {
		return err
	}

	done := make(chan bool)
	go monitorTimeouts(done)
	defer func() { done <- true }()

	glog.Info("Starting map phase...")

	// Wait for map phase completion
	for {
		allMapsDone := true
		masterStateLock.Lock()
		if len(masterState.MapTasks) > 0 {
			allMapsDone = false
		}
		masterStateLock.Unlock()

		if allMapsDone {
			manager, err := GetEtcdManager()
			if err != nil {
				return fmt.Errorf("failed to get etcd manager: %v", err)
			}

			// Check if any workers still processing map tasks
			for _, addr := range workerAddrs {
				worker, err := manager.GetWorkerState(addr)
				if err != nil {
					continue
				}
				if worker.Task != nil {
					if _, isMap := worker.Task.(*pb.Worker_Map); isMap {
						allMapsDone = false
						break
					}
				}
			}
		}

		if allMapsDone {
			glog.Info("All map tasks have completed! Transitioning to reduce phase...")
			updateMasterStateAndBackup(func() {
				masterState.Phase = "reduce"
			})
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	glog.Info("Starting reduce phase...")

	// Wait for reduce phase completion
	for {
		allDone := true
		masterStateLock.Lock()
		if len(masterState.ReduceTasks) > 0 {
			allDone = false
		}
		masterStateLock.Unlock()

		if allDone {
			manager, err := GetEtcdManager()
			if err != nil {
				return fmt.Errorf("failed to get etcd manager: %v", err)
			}

			// Check if any workers still processing tasks
			for _, addr := range workerAddrs {
				worker, err := manager.GetWorkerState(addr)
				if err != nil {
					continue
				}
				if worker.Task != nil {
					allDone = false
					break
				}
			}
		}

		if allDone {
			glog.Info("All reduce tasks have completed! Job finished.")
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	return nil
}

func startHTTPServer(ctx context.Context) error {
	router := mux.NewRouter()
	router.HandleFunc("/api/job", handleJobSubmission).Methods("POST")

	serverMutex.Lock()
	activeServer = &http.Server{
		Addr:    ":30000",
		Handler: router,
	}
	serverMutex.Unlock()

	glog.Infof("Starting HTTP server on :30000, **external port is 30001**")
	return activeServer.ListenAndServe()
}

func stopHTTPServer() {
	serverMutex.Lock()
	defer serverMutex.Unlock()

	if activeServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := activeServer.Shutdown(ctx); err != nil {
			glog.Errorf("Error shutting down HTTP server: %v", err)
		}
		activeServer = nil
	}
}
func runElectionCycle(podName string, sigChan <-chan os.Signal) error {
	glog.V(2).Info("Grabbing etcd client...")
	etcdClient, err := clientv3.New(clientv3.Config{Endpoints: []string{os.Getenv("ETCD_ENDPOINT")}})
	if err != nil {
		return fmt.Errorf("failed to initialize etcd client: %v", err)
	}
	defer etcdClient.Close()

	electionSession, err := concurrency.NewSession(etcdClient, concurrency.WithTTL(10))
	if err != nil {
		return fmt.Errorf("failed to create election session: %v", err)
	}
	defer electionSession.Close()

	election := concurrency.NewElection(electionSession, "/leader-election")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	glog.V(1).Infof("Pod %s attempting to become leader...", podName)
	if err := election.Campaign(ctx, podName); err != nil {
		return fmt.Errorf("failed to campaign for leadership: %v", err)
	}
	glog.Infof("Pod %s became leader", podName)

	// Channel for job completion notification
	jobDone := make(chan bool, 1)
	jobErrCh := make(chan error, 1)

	// Start the HTTP server
	serverErrCh := make(chan error, 1)
	go func() {
		if err := startHTTPServer(ctx); err != nil && err != http.ErrServerClosed {
			serverErrCh <- err
		}
	}()

	// Check for ongoing job and continue it
	if masterState != nil && (len(masterState.MapTasks) > 0 || len(masterState.ReduceTasks) > 0) {
		glog.Infof("Found ongoing job in %s phase, continuing execution...", masterState.Phase)
		printTaskStatus() // Print current system state

		// Continue job execution in a goroutine
		go func() {
			// We need to reinitialize components
			var err error
			workerAddrs, err = initializeWorkers()
			if err != nil {
				jobErrCh <- fmt.Errorf("failed to reinitialize workers: %v", err)
				return
			}

			otherMasters, err = getMasterAddresses()
			if err != nil {
				jobErrCh <- fmt.Errorf("failed to get master addresses: %v", err)
				return
			}

			// Reset task counter for the new leader
			taskCounterLock.Lock()
			taskCounter = 0
			taskCounterLock.Unlock()

			// Start monitoring timeouts which will also handle task assignment
			done := make(chan bool)
			go monitorTimeouts(done)
			defer func() { done <- true }()

			// Continue execution based on current phase
			if masterState.Phase == "map" {
				glog.Info("Continuing map phase...")
				// Wait for map phase completion
				for {
					allMapsDone := true
					masterStateLock.Lock()
					if len(masterState.MapTasks) > 0 {
						allMapsDone = false
					}
					masterStateLock.Unlock()

					if allMapsDone {
						manager, err := GetEtcdManager()
						if err != nil {
							jobErrCh <- fmt.Errorf("failed to get etcd manager: %v", err)
							return
						}

						// Check if any workers still processing map tasks
						for _, addr := range workerAddrs {
							worker, err := manager.GetWorkerState(addr)
							if err != nil {
								continue
							}
							if worker.Task != nil {
								if _, isMap := worker.Task.(*pb.Worker_Map); isMap {
									allMapsDone = false
									break
								}
							}
						}
					}

					if allMapsDone {
						glog.Info("All map tasks have completed! Transitioning to reduce phase...")
						updateMasterStateAndBackup(func() {
							masterState.Phase = "reduce"
						})
						break
					}
					time.Sleep(100 * time.Millisecond)
				}
			}

			// Continue with or start reduce phase
			if masterState.Phase == "reduce" {
				glog.Info("Continuing reduce phase...")
				// Wait for reduce phase completion
				for {
					allDone := true
					masterStateLock.Lock()
					if len(masterState.ReduceTasks) > 0 {
						allDone = false
					}
					masterStateLock.Unlock()

					if allDone {
						manager, err := GetEtcdManager()
						if err != nil {
							jobErrCh <- fmt.Errorf("failed to get etcd manager: %v", err)
							return
						}

						// Check if any workers still processing tasks
						for _, addr := range workerAddrs {
							worker, err := manager.GetWorkerState(addr)
							if err != nil {
								continue
							}
							if worker.Task != nil {
								allDone = false
								break
							}
						}
					}

					if allDone {
						glog.Info("All reduce tasks have completed! Job finished.")
						jobDone <- true
						return
					}
					time.Sleep(100 * time.Millisecond)
				}
			}
		}()
	}

	// Wait for either completion or interruption
	select {
	case <-electionSession.Done():
		glog.Infof("Pod %s lost leadership", podName)
		stopHTTPServer()
	case err := <-serverErrCh:
		glog.Errorf("HTTP server error: %v", err)
		stopHTTPServer()
	case err := <-jobErrCh:
		glog.Errorf("Job execution error: %v", err)
		stopHTTPServer()
	case <-jobDone:
		glog.Info("Job completed successfully")
	case sig := <-sigChan:
		glog.Infof("Received signal %v, resigning leadership", sig)
		stopHTTPServer()
		if err := election.Resign(context.Background()); err != nil {
			glog.Errorf("Failed to resign leadership: %v", err)
		}
		return fmt.Errorf("termination signal")
	}

	return nil
}

func main() {
	flag.Set("logtostderr", "true")
	flag.Set("v", "2")
	flag.Parse()
	defer glog.Flush()

	podName := os.Getenv("POD_NAME")
	if podName == "" {
		glog.Fatalf("POD_NAME environment variable not set")
	}

	// Handle stable master case
	fail, err := strconv.Atoi(os.Getenv("FAIL"))
	if err != nil {
		glog.Errorf("Failed to convert FAIL environment variable to integer: %v", err)
	}
	if fail == 0 {
		// Stable master waits to let unstable master become leader first
		time.Sleep(30 * time.Second)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	// Start gRPC server
	go func() {
		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
		if err != nil {
			glog.Fatalf("failed to listen: %v", err)
		}

		s := grpc.NewServer()
		pb.RegisterMasterMasterServer(s, &MMServer{})
		glog.V(1).Infof("mmserver listening at %v", lis.Addr())

		if err := s.Serve(lis); err != nil {
			glog.Fatalf("failed to serve: %v", err)
		}
	}()

	for {
		err := runElectionCycle(podName, sigChan)
		if err != nil {
			if err.Error() == "termination signal" {
				glog.Info("Shutting down gracefully")
				return
			}
			glog.Errorf("Election cycle error: %v", err)
		}
		glog.Infof("Pod %s restarting election process", podName)
		time.Sleep(5 * time.Second)
	}
}
func shardFiles(numMappers int, numReducers int, inputContainer string) ([]*pb.Shard, error) {
	ctx := context.Background()
	db, err := database.GetStorage(ctx, os.Getenv("AZURE_STORAGE_ACCOUNT"))
	if err != nil {
		glog.Fatal(err.Error())
	}

	blobItems, err := db.ListBlobs(ctx, inputContainer, nil)
	if err != nil {
		glog.Fatalf("Error listing blobs: %v", err)
	}

	var totalInputSize int64
	var files []*pb.File

	for _, blob := range blobItems {
		newFile := &pb.File{
			Name:        *blob.Name,
			Size:        *blob.Properties.ContentLength,
			StartOffset: 0,
			EndOffset:   *blob.Properties.ContentLength,
		}
		files = append(files, newFile)
		glog.V(2).Infof("%s (%d bytes)", newFile.Name, newFile.Size)
		totalInputSize += *blob.Properties.ContentLength
	}

	var shards []*pb.Shard
	targetShardSize := int64(math.Ceil(float64(totalInputSize) / float64(numMappers)))
	glog.V(2).Infof("Total input size: %d bytes, Target shard size: %d bytes", totalInputSize, targetShardSize)

	currentShard := &pb.Shard{ShardId: 1, NumReducers: int64(numReducers), InputContainer: inputContainer}
	currentShardSize := int64(0)

	for fileIndex := 0; fileIndex < len(files); {
		file := files[fileIndex]
		remainingFileSize := file.EndOffset - file.StartOffset

		glog.V(3).Infof("Processing file: %s (remaining size: %d bytes)", file.Name, remainingFileSize)
		glog.V(3).Infof("Current shard size: %d bytes, Target shard size: %d bytes", currentShardSize, targetShardSize)

		if currentShardSize+remainingFileSize <= targetShardSize {
			// Add entire remaining file to current shard
			currentShard.Files = append(currentShard.Files, &pb.File{
				Name:        file.Name,
				Size:        file.Size,
				StartOffset: file.StartOffset,
				EndOffset:   file.EndOffset,
			})
			currentShardSize += remainingFileSize
			glog.V(3).Infof("Added entire file to shard %d. New shard size: %d bytes", currentShard.ShardId, currentShardSize)
			fileIndex++
		} else {
			// Add partial file to current shard
			bytesToAdd := targetShardSize - currentShardSize
			currentShard.Files = append(currentShard.Files, &pb.File{
				Name:        file.Name,
				Size:        file.Size,
				StartOffset: file.StartOffset,
				EndOffset:   file.StartOffset + bytesToAdd,
			})
			file.StartOffset += bytesToAdd
			currentShardSize = targetShardSize
			glog.V(3).Infof("Added partial file to shard %d. Bytes added: %d, New shard size: %d bytes", currentShard.ShardId, bytesToAdd, currentShardSize)
		}

		if currentShardSize >= targetShardSize || fileIndex == len(files) {
			shards = append(shards, currentShard)
			glog.V(3).Infof("Completed shard %d with %d files and total size of %d bytes", currentShard.ShardId, len(currentShard.Files), currentShardSize)
			// new shard
			currentShard = &pb.Shard{ShardId: int64(len(shards) + 1), NumReducers: int64(numReducers), InputContainer: inputContainer}
			currentShardSize = 0
		}
	}

	// Print shard information
	for _, shard := range shards {
		var shardSize int64
		for _, file := range shard.Files {
			shardSize += file.EndOffset - file.StartOffset
		}
		glog.V(2).Infof("Shard %d: %d files, total size: %d bytes", shard.ShardId, len(shard.Files), shardSize)
		for _, file := range shard.Files {
			glog.V(2).Infof(" - (Start: %d, End: %d)", file.StartOffset, file.EndOffset)
			glog.V(2).Infof(" - %s", file.Name)
		}
	}

	return shards, nil
}

// Helper function to initialize workers
func initializeWorkers() ([]string, error) {
	workerService := os.Getenv("WORKER_SERVICE")
	addrs, err := net.LookupHost(workerService)
	if err != nil {
		return nil, fmt.Errorf("failed to lookup worker service: %v", err)
	}

	var workerAddrs []string
	manager, err := GetEtcdManager()
	if err != nil {
		return nil, fmt.Errorf("failed to get etcd manager: %v", err)
	}

	glog.V(2).Infof("Discovering and initializing workers from service %s", workerService)
	for _, addr := range addrs {
		workerAddr := fmt.Sprintf("%s:50051", addr)
		workerAddrs = append(workerAddrs, workerAddr)

		// Initialize worker in etcd if not present
		_, err := manager.GetWorkerState(workerAddr)
		if err != nil && err.Error() == "worker not found" {
			err = manager.UpdateWorkerState(workerAddr, &pb.Worker{
				Task:           nil,
				StartExecution: 0,
			})
			if err != nil {
				glog.Errorf("Failed to initialize worker %s in etcd: %v", workerAddr, err)
				continue
			}
			glog.V(2).Infof("Initialized worker %s in etcd", workerAddr)
		}
	}
	glog.V(2).Infof("Found and initialized %d workers", len(workerAddrs))
	return workerAddrs, nil
}

// Helper function to get master addresses
func getMasterAddresses() ([]string, error) {
	podIP := os.Getenv("POD_IP")
	if podIP == "" {
		glog.Fatalf("POD_IP environment variable not set")
	}

	glog.V(3).Infof("My IP: %s", podIP)

	masterService := os.Getenv("MASTER_SERVICE")
	addrs, err := net.LookupHost(masterService)
	if err != nil {
		return nil, fmt.Errorf("failed to lookup master service: %v", err)
	}

	var masterAddrs []string
	for _, addr := range addrs {
		if addr != podIP {
			masterAddrs = append(masterAddrs, fmt.Sprintf("%s:50052", addr))
		}
	}
	glog.V(2).Infof("Found %d other masters", len(masterAddrs))
	return masterAddrs, nil
}

// GetEtcdManager returns a singleton instance of EtcdManager
func GetEtcdManager() (*EtcdManager, error) {
	once.Do(func() {
		client, err := clientv3.New(clientv3.Config{
			Endpoints:            []string{os.Getenv("ETCD_ENDPOINT")},
			DialTimeout:          5 * time.Second,
			DialKeepAliveTime:    30 * time.Second,
			DialKeepAliveTimeout: 10 * time.Second,
		})
		if err != nil {
			glog.Fatalf("Failed to create etcd client: %v", err)
		}
		etcdManager = &EtcdManager{
			client: client,
		}
	})
	return etcdManager, nil
}

// UpdateWorkerState updates worker state in etcd with optimistic concurrency control
func (em *EtcdManager) UpdateWorkerState(address string, worker *pb.Worker) error {
	em.mu.RLock()
	defer em.mu.RUnlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := em.client.Get(ctx, address)
	if err != nil {
		return fmt.Errorf("failed to get worker state: %v", err)
	}

	newData, err := proto.Marshal(worker)
	if err != nil {
		return fmt.Errorf("failed to marshal worker: %v", err)
	}

	var version int64 = 0
	if len(resp.Kvs) > 0 {
		version = resp.Kvs[0].Version
	}

	txn := em.client.Txn(ctx).If(
		clientv3.Compare(clientv3.Version(address), "=", version),
	).Then(
		clientv3.OpPut(address, string(newData)),
	).Else(
		clientv3.OpGet(address),
	)

	txnResp, err := txn.Commit()
	if err != nil {
		return fmt.Errorf("transaction failed: %v", err)
	}

	if !txnResp.Succeeded {
		return fmt.Errorf("concurrent modification detected")
	}

	return nil
}

// GetWorkerState retrieves worker state from etcd
func (em *EtcdManager) GetWorkerState(address string) (*pb.Worker, error) {
	em.mu.RLock()
	defer em.mu.RUnlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := em.client.Get(ctx, address)
	if err != nil {
		return nil, fmt.Errorf("failed to get worker state: %v", err)
	}

	if len(resp.Kvs) == 0 {
		return nil, fmt.Errorf("worker not found")
	}

	worker := &pb.Worker{}
	if err := proto.Unmarshal(resp.Kvs[0].Value, worker); err != nil {
		return nil, fmt.Errorf("failed to unmarshal worker: %v", err)
	}

	return worker, nil
}

// updateWorkerWithRetry attempts to update worker state with retries
func updateWorkerWithRetry(addr string, update func(*pb.Worker) error) error {
	manager, err := GetEtcdManager()
	if err != nil {
		return err
	}

	for retries := 0; retries < 3; retries++ {
		worker, err := manager.GetWorkerState(addr)
		if err != nil && err.Error() != "worker not found" {
			return err
		}

		if worker == nil {
			worker = &pb.Worker{}
		}

		if err := update(worker); err != nil {
			return err
		}

		err = manager.UpdateWorkerState(addr, worker)
		if err == nil {
			// Log successful update
			if worker.Task != nil {
				if mapTask, isMap := worker.Task.(*pb.Worker_Map); isMap {
					glog.Infof("Updated ETCD worker %s state: Assigned Map task (Shard %d)",
						addr, mapTask.Map.ShardId)
				} else if reduceTask, isReduce := worker.Task.(*pb.Worker_Reduce); isReduce {
					glog.Infof("Updated ETCD worker %s state: Assigned Reduce task (Reducer %d)",
						addr, reduceTask.Reduce.ReducerId)
				}
			} else {
				glog.Infof("Updated worker %s state: Cleared task", addr)
			}
			return nil
		}
		if err.Error() != "concurrent modification detected" {
			return err
		}
		time.Sleep(time.Millisecond * 100)
	}
	return fmt.Errorf("failed to update after 3 retries")
}
