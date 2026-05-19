IMG ?= ghcr.io/aeyrtonvs/k8s-drain-surge
TAG ?= latest
PLATFORMS ?= linux/amd64,linux/arm64

.PHONY: build test vet fmt docker-build docker-push helm-package clean

build:
	go build -o bin/controller ./cmd/controller

test:
	go test ./... -v -race

vet:
	go vet ./...

fmt:
	go fmt ./...

# Single-arch local build (loads into the local docker daemon).
docker-build:
	docker buildx build --platform linux/$(shell go env GOARCH) -t $(IMG):$(TAG) --load .

# Multi-arch build + push as a manifest list. Requires `docker buildx create --use` once.
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
