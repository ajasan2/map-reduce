# MapReduce Go Project

This project implements a reliable and scalable distributed MapReduce system using Go, Kubernetes, and Azure Blob Storage. The system is designed to process large datasets efficiently across multiple nodes, leveraging the power of cloud computing and containerization.

## Project Description

MapReduce is a programming model and processing technique for distributed computing, originally developed by Google. This implementation uses:

- **Go**: For efficient, concurrent processing
- **Kubernetes**: For orchestrating and managing the distributed system
- **Azure Blob Storage**: For storing input data and results
- **gRPC**: For communication between master and worker nodes

The system consists of a master node that coordinates tasks and multiple worker nodes that perform the actual map and reduce operations. This architecture allows for high scalability and fault tolerance.

## Installation

### Prerequisites

1. Go (Golang): [Installation Guide](https://golang.org/doc/install)
2. Docker: [Installation Guide](https://www.digitalocean.com/community/tutorials/how-to-install-and-use-docker-on-ubuntu-18-04)
3. Azure CLI: For deploying to Azure Kubernetes Service (AKS)

### Setup

1. Clone the repository
2. Run the installation script: `./install.sh`

This script will install:
- kubectl
- protobuf
- Kind
- Helm
- gRPC
- Go module (with required dependencies)

3. Add Go to your PATH by adding these lines to your `~/.bashrc`:

```
export PATH=$PATH:/usr/local/go/bin
export PATH="$PATH:$(go env GOPATH)/bin"
```

## Project Structure

- `cmd/`: Contains the main packages for master and worker nodes
- `go.mod`: Defines the project's Go module and dependencies

## Deployment

### Local Deployment

To build and run the project locally: `./build.sh`

This script will:
1. Build gRPC files
2. Compile Go binaries
3. Create Docker images
4. Start a Kubernetes cluster with master and worker pods

### Azure Deployment

To deploy the project on Azure Kubernetes Service (AKS):

1. Ensure you have Azure CLI installed and are logged in
2. Run the deployment script: `./deploy.sh` from the `aks` directory
    -  The script will output the endpoint url of the master, needed when submitting a job to the system.

Follow the prompts to choose deployment options if required.

## Usage

This MapReduce system provides a CLI for managing MapReduce jobs. Here are the main commands:

### Upload Input Files and Scripts

To upload input files and MapReduce scripts:
```
./mapreduce upload --input-dir <path_to_input_files> --mapper <path_to_mapper.py> --reducer <path_to_reducer.py>
```

This command:
- Creates 'input' and 'scripts' containers in Azure Blob Storage
- Uploads all files from the specified input directory to the 'input' container
- Uploads the mapper and reducer scripts to the 'scripts' container

### Submit a MapReduce Job

To submit and configure a new MapReduce job:

```
./mapreduce submit --container <input_container_name> --map-tasks <num_mappers> --reduce-tasks <num_reducers> --master <master_node_url>
```

Options:
- `--container`: Name of the input container (default: "input")
- `--map-tasks`: Number of map tasks (default: 1)
- `--reduce-tasks`: Number of reduce tasks (default: 1)
- `--master`: URL of the master node (default: "http://localhost:30000")

### Download Results

To download the output files after job completion:
```
./mapreduce download --output-dir <path_to_output_directory>
```

This command downloads all files from the 'output' container in Azure Blob Storage to the specified local directory.

### Environment Setup

The Azure storage account should be created in its own resource group prior to deploying the system on AKS. Ensure you have an `azure.env` file in the root directory. This file should contain your Azure Storage account credentials to authenticate with Azure Blob Storage:

```
AZURE_TENANT_ID=your_storage_tenant_id
AZURE_CLIENT_ID=your_storage_client_id
AZURE_CLIENT_SECRET=your_storage_client_secret
AZURE_STORAGE_ACCOUNT=your_storage_account_name
```

In the `deploy.sh` script, the STORAGE_ACCOUNT_NAME and STORAGE_RESOURCE_GROUP will need to be updated based on your storage account.

