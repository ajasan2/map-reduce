cd ..

# Load azure service principal with a secret
if [ -f azure.env ]; then
    export $(cat azure.env | xargs)
else
    echo "azure.env file not found. Please create it with your Azure credentials."
    exit 1
fi

# Build protobuf
protoc --proto_path=protos --go_out=protos --go_opt=paths=source_relative \
        --go-grpc_out=protos --go-grpc_opt=paths=source_relative protos/mapreduce.proto

# Build Go binaries
go build -o cmd/master/master cmd/master/master.go
go build -o cmd/worker/worker cmd/worker/worker.go

# Build Docker images
docker build -t master:latest -f docker/Dockerfile.master .
docker build -t worker:latest -f docker/Dockerfile.worker .

# Setup k8s cluster
kind create cluster --name mapreduce

# Install etcd
helm repo add bitnami https://charts.bitnami.com/bitnami
helm install app-etcd bitnami/etcd --set auth.rbac.create=false

# Load docker images
kind load docker-image master:latest --name mapreduce
kind load docker-image worker:latest --name mapreduce

# Create Kubernetes secret
kubectl create secret generic azure-storage-secret \
    --from-literal=AZURE_TENANT_ID=$AZURE_TENANT_ID \
    --from-literal=AZURE_CLIENT_ID=$AZURE_CLIENT_ID \
    --from-literal=AZURE_CLIENT_SECRET=$AZURE_CLIENT_SECRET \
    --from-literal=AZURE_STORAGE_ACCOUNT=$AZURE_STORAGE_ACCOUNT

kubectl apply -f cmd/worker/worker.yaml
kubectl apply -f cmd/master/master.yaml

# Delete the Go binaries
rm -f cmd/master/master cmd/worker/worker

# Scaling
# kubectl scale deployment worker --replicas=5

# CLI USAGE
# go build -o mapreduce-cli ./cmd/cli/main.go

# ./mapreduce-cli submit \
#   --map-tasks 20 \
#   --reduce-tasks 5 \
#   --master http://172.18.0.2:30001 \
#   --container input-small

#   ./mapreduce-cli submit \
#   --map-tasks 100 \
#   --reduce-tasks 20 \
#   --master http://172.18.0.2:30001 \
#   --container input-big

# Master service IP
# For local, kubectl get nodes -o wide, grab INTERNAL-IP
# For azure, use the IP outputed by ./aks_deploy run

# Cleanup
# kubectl delete deployment master 
# kubectl delete deployment unstable-master
# kubectl delete deployment worker 
# kubectl delete deployment unstable-worker

