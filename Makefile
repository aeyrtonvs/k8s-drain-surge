IMG ?= ghcr.io/aeyrton/k8s-drain-surge
TAG ?= latest

.PHONY: build test vet fmt docker-build docker-push helm-package clean

build:
	go build -o bin/controller ./cmd/controller

test:
	go test ./... -v -race

vet:
	go vet ./...

fmt:
	go fmt ./...

docker-build:
	docker buildx build -t $(IMG):$(TAG) .

docker-push: docker-build
	docker push $(IMG):$(TAG)

helm-package:
	helm package deploy/helm/k8s-drain-surge -d bin/

helm-push: helm-package
	helm push bin/k8s-drain-surge-*.tgz oci://ghcr.io/aeyrton/charts

clean:
	rm -rf bin/

tidy:
	go mod tidy
