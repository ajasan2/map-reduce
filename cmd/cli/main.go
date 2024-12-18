package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"

	"mapreduce/cmd/database"

	"github.com/joho/godotenv"
	"github.com/spf13/cobra"
)

type JobConfig struct {
	NumMappers     int    `json:"num_mappers"`
	NumReducers    int    `json:"num_reducers"`
	InputContainer string `json:"input_container"` // Add this
}

var (
	uploadConfig struct {
		LocalDir    string
		MapperPath  string
		ReducerPath string
	}
	submitConfig struct {
		MapTasks       int
		ReduceTasks    int
		InputContainer string
	}
	downloadConfig struct {
		OutputDir string
	}
)

var rootCmd = &cobra.Command{
	Use:   "mapreduce",
	Short: "A CLI for running MapReduce jobs",
}

var submitCmd = &cobra.Command{
	Use:   "submit",
	Short: "Submit and configure a new MapReduce job",
	RunE:  runSubmit,
}

var uploadCmd = &cobra.Command{
	Use:   "upload",
	Short: "Upload files to Azure Blob Storage",
	RunE:  runUpload,
}

var downloadCmd = &cobra.Command{
	Use:   "download",
	Short: "Download output files from Azure Blob Storage",
	RunE:  runDownload,
}

func init() {
	uploadCmd.Flags().StringVarP(&uploadConfig.LocalDir, "input-dir", "i", "", "Local directory containing input files")
	uploadCmd.Flags().StringVarP(&uploadConfig.MapperPath, "mapper", "m", "", "Path to mapper.py script")
	uploadCmd.Flags().StringVarP(&uploadConfig.ReducerPath, "reducer", "r", "", "Path to reducer.py script")
	uploadCmd.MarkFlagRequired("input-dir")
	uploadCmd.MarkFlagRequired("mapper")
	uploadCmd.MarkFlagRequired("reducer")

	submitCmd.Flags().StringVarP(&submitConfig.InputContainer, "container", "c", "input", "Name of input container")
	submitCmd.Flags().IntVarP(&submitConfig.MapTasks, "map-tasks", "m", 1, "Number of map tasks")
	submitCmd.Flags().IntVarP(&submitConfig.ReduceTasks, "reduce-tasks", "r", 1, "Number of reduce tasks")
	submitCmd.Flags().StringP("master", "u", "http://localhost:30000", "Master node URL")

	downloadCmd.Flags().StringVarP(&downloadConfig.OutputDir, "output-dir", "o", "", "Local directory to store output files")
	downloadCmd.MarkFlagRequired("output-dir")

	// Add commands to root
	rootCmd.AddCommand(submitCmd)
	rootCmd.AddCommand(uploadCmd)
	rootCmd.AddCommand(downloadCmd)
}

func runSubmit(cmd *cobra.Command, args []string) error {
	// Validate task numbers
	if submitConfig.MapTasks < 1 {
		return fmt.Errorf("number of map tasks must be at least 1")
	}
	if submitConfig.ReduceTasks < 1 {
		return fmt.Errorf("number of reduce tasks must be at least 1")
	}

	// Create job configuration
	config := JobConfig{
		NumMappers:     submitConfig.MapTasks,
		NumReducers:    submitConfig.ReduceTasks,
		InputContainer: submitConfig.InputContainer,
	}

	configJson, err := json.Marshal(config)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %v", err)
	}

	// Get master URL from flags
	masterURL, err := cmd.Flags().GetString("master")
	if err != nil {
		return fmt.Errorf("failed to get master URL: %v", err)
	}

	// Send the request
	url := fmt.Sprintf("%s/api/job", masterURL)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(configJson))
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %v", err)
	}
	defer resp.Body.Close()

	fmt.Println("Job submitted successfully")
	return nil
}

func runUpload(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	// Load Azure credentials from azure.env file
	err := godotenv.Load("azure.env")
	if err != nil {
		return fmt.Errorf("error loading azure.env file: %v", err)
	}

	storageName := os.Getenv("AZURE_STORAGE_ACCOUNT")
	if storageName == "" {
		return fmt.Errorf("AZURE_STORAGE_ACCOUNT is not set in azure.env")
	}

	// Initialize Azure Blob Storage client
	storage, err := database.GetStorage(ctx, storageName)
	if err != nil {
		return fmt.Errorf("failed to initialize Azure Storage client: %v", err)
	}

	if err := storage.CreateNewContainer(ctx, "input"); err != nil {
		return fmt.Errorf("failed to create the %s container: %v", "input", err)
	}
	if err := storage.CreateNewContainer(ctx, "scripts"); err != nil {
		return fmt.Errorf("failed to create the %s container: %v", "scripts", err)
	}

	// Read files from the local directory
	files, err := os.ReadDir(uploadConfig.LocalDir)
	if err != nil {
		return fmt.Errorf("failed to read local directory: %v", err)
	}

	for _, file := range files {
		if file.IsDir() {
			continue // Skip directories
		}

		filePath := filepath.Join(uploadConfig.LocalDir, file.Name())
		data, err := os.ReadFile(filePath)
		if err != nil {
			return fmt.Errorf("failed to read file %s: %v", file.Name(), err)
		}

		if err := storage.UploadBlob(ctx, "input", file.Name(), data); err != nil {
			return fmt.Errorf("failed to upload file %s: %v", file.Name(), err)
		}

		fmt.Printf("Successfully uploaded %s\n", file.Name())
	}

	// Upload scripts
	data, err := os.ReadFile(uploadConfig.MapperPath)
	if err != nil {
		return fmt.Errorf("failed to read file %s: %v", uploadConfig.MapperPath, err)
	}

	if err := storage.UploadBlob(ctx, "scripts", "mapper.py", data); err != nil {
		return fmt.Errorf("failed to upload file %s: %v", uploadConfig.MapperPath, err)
	}
	fmt.Printf("Successfully uploaded mapper script\n")

	data, err = os.ReadFile(uploadConfig.ReducerPath)
	if err != nil {
		return fmt.Errorf("failed to read file %s: %v", uploadConfig.ReducerPath, err)
	}

	if err := storage.UploadBlob(ctx, "scripts", "reducer.py", data); err != nil {
		return fmt.Errorf("failed to upload file %s: %v", uploadConfig.ReducerPath, err)
	}
	fmt.Printf("Successfully uploaded reducer script\n")

	return nil
}

func runDownload(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	// Load Azure credentials from azure.env file
	err := godotenv.Load("azure.env")
	if err != nil {
		return fmt.Errorf("error loading azure.env file: %v", err)
	}

	storageName := os.Getenv("AZURE_STORAGE_ACCOUNT")
	if storageName == "" {
		return fmt.Errorf("AZURE_STORAGE_ACCOUNT is not set in azure.env")
	}

	// Initialize Azure Blob Storage client
	storage, err := database.GetStorage(ctx, storageName)
	if err != nil {
		return fmt.Errorf("failed to initialize Azure Storage client: %v", err)
	}

	// Create output directory if it doesn't exist
	if err := os.MkdirAll(downloadConfig.OutputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %v", err)
	}

	// List blobs in the output container
	blobItems, err := storage.ListBlobs(ctx, "output", nil)
	if err != nil {
		return fmt.Errorf("failed to list blobs in output container: %v", err)
	}

	for _, blob := range blobItems {
		localPath := filepath.Join(downloadConfig.OutputDir, *blob.Name)

		// Download the blob content
		buffer := make([]byte, *blob.Properties.ContentLength)
		_, err = storage.DownloadBlobToBuffer(ctx, "output", *blob.Name, buffer, nil)
		if err != nil {
			return fmt.Errorf("failed to download blob %s: %v", *blob.Name, err)
		}

		// Write the content to a local file
		err = os.WriteFile(localPath, buffer, 0644)
		if err != nil {
			return fmt.Errorf("failed to write file %s: %v", localPath, err)
		}

		fmt.Printf("Successfully downloaded %s\n", *blob.Name)
	}

	fmt.Println("All output files downloaded successfully")
	return nil
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
