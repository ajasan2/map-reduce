package main

import (
	"bufio"
	"bytes"
	"context"
	"flag"
	"fmt"
	"hash/fnv"
	"log"
	"net"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"mapreduce/cmd/database"
	pb "mapreduce/protos"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/golang/glog"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
)

var (
	port = flag.Int("port", 50051, "The server port")
)

type Server struct {
	pb.UnimplementedMasterWorkerServer
	addr           string
	taskCounter    int
	completeNTasks int
	etcdManager    *EtcdManager
}
type EtcdManager struct {
	client *clientv3.Client
	mu     sync.RWMutex
}

var (
	etcdManager *EtcdManager
	once        sync.Once
)

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
func (s *Server) ExecuteMap(masterCtx context.Context, inShard *pb.Shard) (*pb.Response, error) {
	// Immediately acknowledge task to master
	glog.V(2).Infof("Received Map request for shard ID: %d", inShard.ShardId)

	// Create our own independent context for the actual work
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)

	// Start task in separate goroutine
	go func() {
		defer cancel()

		s.taskCounter += 1
		if s.completeNTasks > 0 && s.taskCounter >= s.completeNTasks {
			glog.V(1).Info("FAILED (intentionally)")
			os.Exit(0)
		}

		db, err := database.GetStorage(ctx, os.Getenv("AZURE_STORAGE_ACCOUNT"))
		if err != nil {
			glog.Errorf("Failed to connect to Azure blob storage: %v", err)
			return
		}

		scriptBuffer := make([]byte, 1024*1024)
		bytesRead, err := db.DownloadBlobToBuffer(ctx, "scripts", "mapper.py", scriptBuffer, nil)
		if err != nil {
			glog.Errorf("Failed to download mapper script: %v", err)
			return
		}

		tmpScript, err := os.CreateTemp("", "mapper_*.py")
		if err != nil {
			glog.Errorf("Failed to create temp script: %v", err)
			return
		}
		defer os.Remove(tmpScript.Name())

		if err := os.WriteFile(tmpScript.Name(), scriptBuffer[:bytesRead], 0755); err != nil {
			glog.Errorf("Failed to write mapper script: %v", err)
			return
		}

		var aggregatedData []byte

		for i, file := range inShard.Files {
			glog.V(3).Infof("Processing file %d/%d: %s (offset: %d to %d)", i+1, len(inShard.Files), file.Name, file.StartOffset, file.EndOffset)
			fileData, err := s.processFileChunk(ctx, db, file, inShard.InputContainer)
			if err != nil {
				glog.Errorf("Error processing file %s: %v", file.Name, err)
				return
			}

			aggregatedData = append(aggregatedData, fileData...)
			glog.V(3).Infof("Finished processing file %s, current aggregate size: %d",
				file.Name, len(aggregatedData))
		}

		cmd := exec.CommandContext(ctx, "python3", tmpScript.Name())
		stdin, err := cmd.StdinPipe()
		if err != nil {
			glog.Errorf("Failed to create stdin pipe: %v", err)
			return
		}

		var stdout, stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr

		if err := cmd.Start(); err != nil {
			glog.Errorf("Failed to start mapper: %v", err)
			return
		}

		go func() {
			defer stdin.Close()
			stdin.Write(aggregatedData)
		}()

		if err := cmd.Wait(); err != nil {
			glog.Errorf("Mapper failed: %v, stderr: %s", err, stderr.String())
			return
		}

		// Parse TSV output and partition by reducer
		partitionedOutput := make([][]string, inShard.NumReducers+1)
		scanner := bufio.NewScanner(&stdout)

		for scanner.Scan() {
			line := scanner.Text()
			parts := strings.Split(line, "\t")
			if len(parts) != 2 {
				continue
			}
			word := parts[0]

			hash := fnv.New32a()
			hash.Write([]byte(word))
			reducerID := (int(hash.Sum32()) % int(inShard.NumReducers)) + 1

			partitionedOutput[reducerID] = append(partitionedOutput[reducerID], line)
		}

		for reducerID := 1; reducerID < len(partitionedOutput); reducerID++ {
			lines := partitionedOutput[reducerID]
			if len(lines) == 0 {
				continue
			}

			outputData := []byte(strings.Join(lines, "\n") + "\n")
			intermediateName := fmt.Sprintf("intermediate_%d.txt", reducerID)
			err = db.AppendToBlob(ctx, "intermediate", intermediateName, outputData)
			if err != nil {
				glog.Errorf("Failed to append to partition %d: %v", reducerID, err)
				return
			}

			glog.V(3).Infof("Appended %d lines to reducer partition %d", len(lines), reducerID)
		}

		glog.V(2).Infof("Finished processing all files for shard ID: %d", inShard.ShardId)

		// Update etcd state after completion
		worker, err := s.etcdManager.GetWorkerState(s.addr)
		if err != nil {
			if err.Error() != "worker not found" {
				glog.Errorf("Failed to get worker state: %v", err)
				return
			}
			worker = &pb.Worker{}
			glog.V(2).Infof("Initializing new worker state for %s", s.addr)
		}

		glog.V(2).Infof("Worker %s state BEFORE updating etcd:\n%s", s.addr, prototext.Format(worker))

		// Clear the task and execution time
		worker.Task = nil
		worker.StartExecution = 0

		// Update etcd
		if err := s.etcdManager.UpdateWorkerState(s.addr, worker); err != nil {
			glog.Errorf("Failed to update worker state: %v", err)
			return
		}

		glog.V(2).Infof("Worker %s state successfully updated in etcd - Task: nil, StartExecution: 0", s.addr)
	}()

	// Return success immediately while the work continues in background
	return &pb.Response{
		Outcome: "ACCEPTED",
	}, nil
}

func (s *Server) processFileChunk(ctx context.Context, db *database.Storage, file *pb.File, inputContainer string) ([]byte, error) {
	padding := 0
	if file.EndOffset != file.Size {
		padding = 100
	}

	bufferSize := file.EndOffset - file.StartOffset + int64(padding)
	buffer := make([]byte, bufferSize)
	options := &azblob.DownloadBufferOptions{
		Range: azblob.HTTPRange{
			Offset: file.StartOffset,
			Count:  bufferSize,
		},
	}

	_, err := db.DownloadBlobToBuffer(ctx, inputContainer, file.Name, buffer, options)
	if err != nil {
		glog.Errorf("Error downloading file %s: %v", file.Name, err)
		return nil, fmt.Errorf("error downloading file %s: %v", file.Name, err)
	}

	newStartOffset, newEndOffset, err := s.adjustBoundaries(file, buffer, padding)
	if err != nil {
		glog.Errorf("Error adjusting boundaries for file %s: %v", file.Name, err)
		return nil, fmt.Errorf("error adjusting boundaries for file %s: %v", file.Name, err)
	}

	return buffer[newStartOffset:newEndOffset], nil
}

func (s *Server) adjustBoundaries(file *pb.File, buffer []byte, padding int) (int64, int64, error) {
	wordRegex := regexp.MustCompile(`[A-Za-z0-9](?:[A-Za-z0-9'''-]*[A-Za-z0-9])?`)

	startIndex := 0
	if file.StartOffset != 0 {
		glog.V(3).Infof("Updating start offset...")
		glog.V(3).Infof("[PRE]  First 10 characters: %s", string(buffer[startIndex:startIndex+11]))

		loc := wordRegex.FindIndex(buffer)
		if loc != nil {
			word := string(buffer[startIndex+loc[0] : startIndex+loc[1]])
			glog.V(3).Infof("Found word: %s, adding its end index (%d) to offset", word, loc[1])
			startIndex += loc[1]
			glog.V(3).Infof("[POST] First 10 characters: %s", string(buffer[startIndex:startIndex+11]))
		} else {
			glog.V(3).Infof("No word found from start offset %d", startIndex)
		}
	}

	endIndex := len(buffer) - padding
	if file.EndOffset != file.Size {
		glog.V(3).Infof("Updating end offset...")
		glog.V(3).Infof("[PRE]  Last 10 characters: %s", string(buffer[endIndex-10:endIndex]))

		loc := wordRegex.FindIndex(buffer[endIndex:])
		if loc != nil {
			word := string(buffer[endIndex+loc[0] : endIndex+loc[1]])
			glog.V(3).Infof("Found word: %s, adding its end index (%d) to offset", word, loc[1])
			endIndex += loc[1]
			glog.V(3).Infof("[POST] Last 10 characters: %s", string(buffer[endIndex-10:endIndex]))
		} else {
			glog.V(3).Infof("No word found from end offset %d", endIndex)
		}
	}

	glog.V(3).Infof("Adjusted chunk size: %d bytes", endIndex-startIndex)
	return int64(startIndex), int64(endIndex), nil
}
func (s *Server) ExecuteReduce(masterCtx context.Context, req *pb.ReduceRequest) (*pb.Response, error) {
	glog.V(2).Infof("Received Reduce request on Reducer: %d", req.ReducerId)
	reducerID := req.ReducerId

	// Create independent context
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)

	// Start work in separate goroutine
	go func() {
		defer cancel()

		s.taskCounter += 1
		if s.completeNTasks > 0 && s.taskCounter >= s.completeNTasks {
			glog.V(1).Info("FAILED (intentionally)")
			os.Exit(0)
		}

		db, err := database.GetStorage(ctx, os.Getenv("AZURE_STORAGE_ACCOUNT"))
		if err != nil {
			glog.Errorf("Failed to connect to Azure blob storage: %v", err)
			return
		}

		intermediateFileName := fmt.Sprintf("intermediate_%d.txt", reducerID)
		data, err := db.DownloadAppendBlobToBuffer(ctx, "intermediate", intermediateFileName)
		if err != nil {
			glog.Errorf("Error downloading append blob %s: %v", intermediateFileName, err)
			return
		}

		scriptBuffer := make([]byte, 1024*1024)
		bytesReadScript, err := db.DownloadBlobToBuffer(ctx, "scripts", "reducer.py", scriptBuffer, nil)
		if err != nil {
			glog.Errorf("Failed to download reducer script: %v", err)
			return
		}

		tmpScript, err := os.CreateTemp("", "reducer_*.py")
		if err != nil {
			glog.Errorf("Failed to create temp script: %v", err)
			return
		}
		defer os.Remove(tmpScript.Name())

		if err := os.WriteFile(tmpScript.Name(), scriptBuffer[:bytesReadScript], 0755); err != nil {
			glog.Errorf("Failed to write reducer script: %v", err)
			return
		}

		cmd := exec.CommandContext(ctx, "python3", tmpScript.Name())
		stdin, err := cmd.StdinPipe()
		if err != nil {
			glog.Errorf("Failed to create stdin pipe: %v", err)
			return
		}
		var stdout, stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr

		if err := cmd.Start(); err != nil {
			glog.Errorf("Failed to start reducer: %v", err)
			return
		}

		go func() {
			defer stdin.Close()
			stdin.Write(data)
		}()

		if err := cmd.Wait(); err != nil {
			glog.Errorf("Reducer failed: %v, stderr: %s", err, stderr.String())
			return
		}

		glog.V(2).Infof("Reducer script executed successfully for reducer ID %d", reducerID)

		outputData := stdout.Bytes()
		outputFileName := fmt.Sprintf("reduced_%d.txt", reducerID)

		err = db.CreateNewContainer(ctx, "output")
		if err != nil {
			glog.Errorf("Error creating output container %s: %v", "output", err)
			return
		}

		err = db.UploadBlob(ctx, "output", outputFileName, outputData)
		if err != nil {
			glog.Errorf("Error uploading output file %s: %v", outputFileName, err)
			return
		}

		glog.V(2).Infof("Finished processing reducer ID %d, output saved to %s", reducerID, outputFileName)

		// Get current worker state from etcd
		worker, err := s.etcdManager.GetWorkerState(s.addr)
		if err != nil {
			if err.Error() != "worker not found" {
				glog.Errorf("Failed to get worker state: %v", err)
				return
			}
			worker = &pb.Worker{}
			glog.V(2).Infof("Initializing new worker state for %s", s.addr)
		}

		glog.V(2).Infof("Worker %s state BEFORE updating etcd:\n%s", s.addr, prototext.Format(worker))

		// Clear the task and execution time
		worker.Task = nil
		worker.StartExecution = 0

		// Update etcd
		if err := s.etcdManager.UpdateWorkerState(s.addr, worker); err != nil {
			glog.Errorf("Failed to update worker state: %v", err)
			return
		}

		glog.V(2).Infof("Worker %s state successfully updated in etcd - Task: nil, StartExecution: 0", s.addr)
	}()

	// Return success immediately while the work continues in background
	return &pb.Response{
		Outcome: "ACCEPTED",
	}, nil
}

func main() {
	flag.Set("logtostderr", "true")
	flag.Set("v", "2")
	flag.Parse()
	defer glog.Flush()

	podIP := os.Getenv("POD_IP")
	if podIP == "" {
		glog.Fatalf("POD_IP environment variable not set")
	}
	glog.V(2).Infof("My IP: %s", podIP)

	// Initialize etcd manager
	manager, err := GetEtcdManager()
	if err != nil {
		glog.Fatalf("Failed to initialize etcd manager: %v", err)
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	n_task, _ := strconv.Atoi(os.Getenv("FAIL"))
	s := grpc.NewServer()
	pb.RegisterMasterWorkerServer(s, &Server{
		addr:           fmt.Sprintf("%s:50051", podIP),
		completeNTasks: n_task,
		taskCounter:    0,
		etcdManager:    manager,
	})

	glog.Infof("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		glog.Fatalf("failed to serve: %v", err)
	}
}
