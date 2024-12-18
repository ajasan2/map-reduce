package database

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/golang/glog"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/log"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
)

func init() {
	// Enable specific log events
	log.SetEvents(log.EventResponseError)

	// Set a custom listener that uses glog
	log.SetListener(func(event log.Event, message string) {
		glog.V(3).Infof("[%s] %s", event, message)
	})
}

type Storage struct {
	Name   string
	Client *azblob.Client
}

func GetStorage(ctx context.Context, name string) (*Storage, error) {
	glog.V(1).Infof("Connecting to Azure storage %s", name)
	url := fmt.Sprintf("https://%s.blob.core.windows.net/", name)

	credential, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, fmt.Errorf("failed azidentity with default credentials: %v", err)
	}

	client, err := azblob.NewClient(url, credential, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get azblob client: %v", err)
	}

	return &Storage{
		Name:   name,
		Client: client,
	}, nil
}

func (s *Storage) CreateNewContainer(ctx context.Context, containerName string) error {
	glog.V(2).Infof("Creating the %s container", containerName)

	_, err := s.Client.CreateContainer(ctx, containerName, nil)
	if err != nil {
		if bloberror.HasCode(err, bloberror.ContainerAlreadyExists) {
			glog.V(3).Infof("Container %s already exists", containerName)
			return nil
		}
		return err
	}

	glog.V(3).Info("Successfully created container")
	return nil
}

func (s *Storage) UploadBlob(ctx context.Context, containerName string, blobName string, data []byte) error {
	glog.V(2).Infof("Uploading new blob %s in the %s container", blobName, containerName)

	// Create reader from byte slice
	reader := bytes.NewReader(data)
	readSeekCloser := struct {
		io.Seeker
		io.ReadCloser
	}{
		Seeker:     reader,
		ReadCloser: io.NopCloser(reader),
	}

	// Get clients
	containerClient := s.Client.ServiceClient().NewContainerClient(containerName)
	blobClient := containerClient.NewBlockBlobClient(blobName)

	// Upload the data
	_, err := blobClient.Upload(ctx, readSeekCloser, &blockblob.UploadOptions{})
	if err != nil {
		return err
	}

	glog.V(3).Info("Successfully uploaded blob")
	return nil
}

func (s *Storage) ListBlobs(ctx context.Context, containerName string, options *azblob.ListBlobsFlatOptions) ([]*container.BlobItem, error) {
	glog.V(2).Infof("Listing blobs in the %s container", containerName)

	var blobItems []*container.BlobItem
	pager := s.Client.NewListBlobsFlatPager(containerName, options)

	for pager.More() {
		resp, err := pager.NextPage(ctx)
		if err != nil {
			return nil, err
		}

		if resp.Segment != nil && resp.Segment.BlobItems != nil {
			blobItems = append(blobItems, resp.Segment.BlobItems...)
		}
	}

	glog.V(3).Infof("Found %d blobs in %s", len(blobItems), containerName)
	return blobItems, nil
}

func (s *Storage) DownloadBlobToBuffer(ctx context.Context, containerName string, blobName string, buffer []byte, o *azblob.DownloadBufferOptions) (int64, error) {
	glog.V(2).Infof("Downloading %s from the %s container", blobName, containerName)

	bytes, err := s.Client.DownloadBuffer(ctx, containerName, blobName, buffer, o)
	if err != nil {
		return 0, err
	}

	glog.V(3).Infof("Successfully downloaded (%d bytes)", bytes)
	return bytes, nil
}

func (s *Storage) GetBlobSize(ctx context.Context, containerName string, blobName string) (int64, error) {
	glog.V(2).Infof("Getting properties for %s in the %s container", blobName, containerName)

	props, err := s.Client.ServiceClient().NewContainerClient(containerName).NewBlobClient(blobName).GetProperties(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("error getting properties: %v", err)
	}

	glog.V(3).Info("Successfully retrieved properties")
	return *props.ContentLength, nil
}

func (s *Storage) CreateAppendBlob(ctx context.Context, containerName string, blobName string) error {
	glog.V(2).Infof("Creating append blob %s in the %s container", blobName, containerName)

	// Get clients
	containerClient := s.Client.ServiceClient().NewContainerClient(containerName)
	appendBlobClient := containerClient.NewAppendBlobClient(blobName)

	// Create the append blob
	_, err := appendBlobClient.Create(ctx, nil)
	if err != nil {
		if bloberror.HasCode(err, bloberror.BlobAlreadyExists) {
			glog.V(3).Info("Append blob already exists")
			return nil
		}
		return err
	}

	glog.V(3).Info("Successfully created append blob")
	return nil
}

// func (s *Storage) AppendToBlob(ctx context.Context, containerName string, blobName string, data []byte) error {
// 	if len(data) == 0 {
// 		return fmt.Errorf("cannot append empty data")
// 	}

// 	glog.V(2).Infof("Appending (%d bytes) to blob %s in the %s container", len(data), blobName, containerName)

// 	// Get clients
// 	containerClient := s.Client.ServiceClient().NewContainerClient(containerName)
// 	appendBlobClient := containerClient.NewAppendBlobClient(blobName)

// 	// Create lease client and acquire lease
// 	glog.V(3).Info("Acquiring lease")
// 	blobLeaseClient, err := lease.NewBlobClient(appendBlobClient.BlobClient(), nil)
// 	if err != nil {
// 		return fmt.Errorf("failed to create lease client: %v", err)
// 	}

// 	resp, err := blobLeaseClient.AcquireLease(ctx, 15, nil) // 15 second lease
// 	if err != nil {
// 		return fmt.Errorf("failed to acquire lease: %v", err)
// 	}
// 	leaseID := *resp.LeaseID
// 	glog.V(3).Infof("Successfully acquired lease: ", leaseID)

// 	// Make sure we release the lease when we're done
// 	defer func() {
// 		if _, err := blobLeaseClient.ReleaseLease(ctx, nil); err != nil {
// 			glog.Errorf("Failed to release lease %s on blob %s: %v", leaseID, blobName, err)
// 		} else {
// 			glog.V(3).Infof("Successfully released lease: %s", leaseID)
// 		}
// 	}()

// 	// Append the data with the lease
// 	reader := bytes.NewReader(data)
// 	options := &appendblob.AppendBlockOptions{
// 		AccessConditions: &blob.AccessConditions{
// 			LeaseAccessConditions: &blob.LeaseAccessConditions{
// 				LeaseID: &leaseID,
// 			},
// 		},
// 	}

// 	_, err = appendBlobClient.AppendBlock(ctx, streaming.NopCloser(reader), options)
// 	if err != nil {
// 		return fmt.Errorf("failed to append data with lease %s: %v", leaseID, err)
// 	}

// 	glog.V(3).Info("Successfully appended to blob")
// 	return nil
// }

func (s *Storage) AppendToBlob(ctx context.Context, containerName string, blobName string, data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("cannot append empty data")
	}

	glog.V(2).Infof("Appending (%d bytes) to blob %s in the %s container", len(data), blobName, containerName)

	// Get clients
	containerClient := s.Client.ServiceClient().NewContainerClient(containerName)
	appendBlobClient := containerClient.NewAppendBlobClient(blobName)

	// Append the data
	reader := bytes.NewReader(data)
	_, err := appendBlobClient.AppendBlock(ctx, streaming.NopCloser(reader), nil)
	if err != nil {
		return fmt.Errorf("failed to append data: %v", err)
	}

	glog.V(3).Info("Successfully appended to blob")
	return nil
}

func (s *Storage) DownloadAppendBlobToBuffer(ctx context.Context, containerName string, blobName string) ([]byte, error) {
	glog.V(2).Infof("Downloading blob %s from the %s container", blobName, containerName)

	// Get clients
	containerClient := s.Client.ServiceClient().NewContainerClient(containerName)
	appendBlobClient := containerClient.NewAppendBlobClient(blobName)

	// Download the stream
	response, err := appendBlobClient.DownloadStream(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get download stream: %v", err)
	}

	// Read the entire stream into a buffer
	buffer := &bytes.Buffer{}
	reader := response.Body
	_, err = buffer.ReadFrom(reader)
	if err != nil {
		reader.Close()
		return nil, fmt.Errorf("failed to read blob data: %v", err)
	}

	if err := reader.Close(); err != nil {
		return nil, fmt.Errorf("failed to close reader: %v", err)
	}

	glog.V(3).Infof("Successfully downloaded (%d bytes)", buffer.Len())
	return buffer.Bytes(), nil
}
