#!/bin/bash

# Define variables for image and container names
IMAGE_NAME="bagels_local"
CONTAINER_NAME="local_dev_container"
DOCKERFILE="Dockerfile.local"
HOST_PORT=8000  # Change this if you need a different port
CONTAINER_PORT=8000  # Should match the port set in uvicorn (default 8000)

# Check if the -b or --build flag is provided
BUILD_IMAGE=false
for arg in "$@"; do
  if [[ "$arg" == "-b" || "$arg" == "--build" ]]; then
    BUILD_IMAGE=true
  fi
done

# Build the Docker image if the flag is set or if the image doesn't exist
if $BUILD_IMAGE || [ -z "$(docker images -q $IMAGE_NAME)" ]; then
    echo "Building the Docker image..."
    docker build -t $IMAGE_NAME -f $DOCKERFILE .
else
    echo "Skipping build. Using existing Docker image: $IMAGE_NAME"
fi

# Check if a container with the same name is already running and remove it
if [ "$(docker ps -aq -f name=$CONTAINER_NAME)" ]; then
    echo "Stopping and removing existing container..."
    docker stop $CONTAINER_NAME
    docker rm $CONTAINER_NAME
fi

# Run the Docker container with volume mounting
echo "Running the Docker container..."
docker run -d --name $CONTAINER_NAME -p $HOST_PORT:$CONTAINER_PORT \
    -v "$(pwd)":/app $IMAGE_NAME

echo "Container started successfully. Access the app at http://localhost:$HOST_PORT"
