IMG ?= ghcr.io/aeyrtonvs/k8s-drain-surge
TAG ?= latest
PLATFORMS ?= linux/amd64,linux/arm64

.PHONY: build test vet fmt tidy docker-build docker-push helm-package helm-push clean

build:
	go build -o bin/controller ./cmd/controller

test:
	go test ./... -v -race

vet:
	go vet ./...

fmt:
	go fmt ./...

# --load can only emit a single-arch image into the local daemon, so this
# build defaults to the host arch. Use docker-push for multi-arch releases.
docker-build:
	docker buildx build -t $(IMG):$(TAG) --load .

# Requires `docker buildx create --use` once to create a container-driver builder.
docker-push:
	docker buildx build --platform $(PLATFORMS) -t $(IMG):$(TAG) --push .

helm-package:
	helm package deploy/helm/k8s-drain-surge -d bin/

helm-push: helm-package
	helm push bin/k8s-drain-surge-*.tgz oci://ghcr.io/aeyrtonvs/charts

clean:
	rm -rf bin/

tidy:
	go mod tidy
