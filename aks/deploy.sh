#!/bin/bash
cd ..

# Set variables
RESOURCE_GROUP="sdcc-map-reduce-aks"
CLUSTER_NAME="mapreducecluster"
LOCATION="eastus"
NODE_COUNT=1
ACR_NAME="mapreducecontainers"
IDENTITY_NAME="mapreduceidentity"
STORAGE_ACCOUNT_NAME="mapreduceblob"
STORAGE_RESOURCE_GROUP="sdcc-map-reduce"

# Function to register resource providers
register_providers() {
    providers=("Microsoft.ContainerService" "Microsoft.Network" "Microsoft.Storage" "Microsoft.Compute" "Microsoft.ContainerRegistry" "Microsoft.ServiceLinker" "Microsoft.KubernetesConfiguration")
    for provider in "${providers[@]}"; do
        echo "Registering $provider..."
        az provider register --namespace $provider
        while [ "$(az provider show -n $provider --query registrationState -o tsv)" != "Registered" ]; do
            echo "Waiting for $provider to register..."
            sleep 20
        done
    done
}

# Function to create and configure ACR
create_acr() {
    echo "Creating Azure Container Registry..."
    if ! az acr show --name $ACR_NAME --resource-group $RESOURCE_GROUP &>/dev/null; then
        az acr create --resource-group $RESOURCE_GROUP --name $ACR_NAME --sku Basic
    fi

    echo "Building and pushing master image..."
    az acr build --registry $ACR_NAME --image master:latest --file docker/Dockerfile.master .

    echo "Building and pushing worker image..."
    az acr build --registry $ACR_NAME --image worker:latest --file docker/Dockerfile.worker .
}

# Function to create the AKS cluster
create_cluster() {
    echo "Creating resource group..."
    az group create --name $RESOURCE_GROUP --location $LOCATION
    create_acr
    
    # Create a user-assigned managed identity and retrieve its ID
    az identity create --name $IDENTITY_NAME --resource-group $RESOURCE_GROUP
    IDENTITY_RESOURCE_ID=$(az identity show --name $IDENTITY_NAME --resource-group $RESOURCE_GROUP --query id -o tsv)

    echo "Creating AKS cluster..."
    az aks create \
        --resource-group $RESOURCE_GROUP \
        --name $CLUSTER_NAME \
        --node-count $NODE_COUNT \
        --enable-managed-identity \
        --assign-identity $IDENTITY_RESOURCE_ID \
        --generate-ssh-keys \
        --attach-acr $(az acr show --name $ACR_NAME --resource-group $RESOURCE_GROUP --query id --output tsv) \
        --enable-workload-identity \
        --enable-oidc-issuer

    echo "Configuring kubectl..."
    az aks get-credentials --resource-group $RESOURCE_GROUP --name $CLUSTER_NAME --overwrite-existing

    # Create service connection using Service Connector
    echo "Creating service connection using Service Connector..."
    az aks connection create storage-blob \
        --resource-group $RESOURCE_GROUP \
        --name $CLUSTER_NAME \
        --target-resource-group $STORAGE_RESOURCE_GROUP \
        --account $STORAGE_ACCOUNT_NAME \
        --workload-identity $IDENTITY_RESOURCE_ID

    # Verify configuration
    if kubectl get nodes &> /dev/null; then
        echo "kubectl successfully configured."
    else
        echo "Failed to configure kubectl. Please check your AKS cluster and try again."
        exit 1
    fi
}

apply_manifest() {
    cd aks
    IDENTITY_CLIENT_ID=$(az identity show --name $IDENTITY_NAME --resource-group $RESOURCE_GROUP --query clientId -o tsv)
    SERVICE_ACCOUNT_NAME="sc-account-$IDENTITY_CLIENT_ID"

    TEMP_MANIFEST=$(mktemp)
    cp manifest.yaml "$TEMP_MANIFEST"

    sed -i "s|\${USER_ASSIGNED_IDENTITY_CLIENT_ID}|$IDENTITY_CLIENT_ID|g" "$TEMP_MANIFEST"
    sed -i "s|\${SERVICE_ACCOUNT_BY_CONNECTOR}|$SERVICE_ACCOUNT_NAME|g" "$TEMP_MANIFEST"
    
    kubectl apply -f "$TEMP_MANIFEST"
    rm "$TEMP_MANIFEST"

    # Wait for the master-service to get an external IP
    while : ; do
        MASTER_IP=$(kubectl get service master-service --no-headers -o custom-columns=":status.loadBalancer.ingress[0].ip")
        if [[ -n "$MASTER_IP" ]]; then
            break
        fi
        sleep 10
    done

    echo "Master service is available at: http://$MASTER_IP:30001"
}

# Function to install etcd
install_etcd() {
    echo "Installing etcd using Helm..."
    helm repo add bitnami https://charts.bitnami.com/bitnami
    helm repo update
    helm install app-etcd bitnami/etcd --set auth.rbac.create=false
    echo "etcd installation complete."
}

# Function to delete the AKS cluster and ACR
delete_cluster() {
    echo "Deleting AKS cluster and ACR..."
    az group delete --name $RESOURCE_GROUP --yes --no-wait
    az group wait --name $RESOURCE_GROUP --deleted

    if [ $? -eq 0 ]; then
        echo "AKS has been successfully deleted."
    else
        echo "Failed to delete the resource group. Please check the Azure portal for more details."
    fi
}

# Main script
case "$1" in
    up)
        register_providers
        create_cluster
        install_etcd
        ;;
    run)
        apply_manifest
        ;;
    down)
        delete_cluster
        ;;
    *)
        echo "Usage: $0 <up|run|down>"
        exit 1
        ;;
esac
