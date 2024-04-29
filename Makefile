.PHONY: default image push-image image-plateau push-image-plateau check-fmt fmt lint clean

TAG                      ?= $(shell git rev-parse --short  HEAD)
IMAGE_ROOT                = ghcr.io/wallaroolabs
PLATEAU_IMAGE  			  = $(IMAGE_ROOT)/plateau
DOCKER                    = docker buildx
REVISION                  = $(shell git describe --match="" --always --abbrev=40 --dirty)
TIME                      = $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
RUST_VERSION              = 1.76.0

test:
	cd upstream && cargo test --workspace --features test,batch,polars -- --nocapture

fmt: check-fmt

check-fmt:
	cd upstream && cargo fmt --all -- --check

lint:
	cd upstream && cargo clippy --workspace --all-targets --features batch,polars,test -- -D warnings

coverage-%:
	@$(MAKE) -C upstream -f ../../rust-grcov.mk $@

clean:
	cd upstream && cargo clean
	cd upstream/server/bench && cargo clean

image: image-plateau

include ../sccache.mk

image-plateau:
	$(DOCKER) build \
	--build-arg RUST_VERSION=$(RUST_VERSION) \
	--build-arg BUILD_COMMIT=$(REVISION) \
	-t $(PLATEAU_IMAGE):$(TAG) -t $(PLATEAU_IMAGE):latest \
	--cache-to=type=registry,ref=$(PLATEAU_IMAGE):cache,mode=max \
	--cache-from=type=registry,ref=$(PLATEAU_IMAGE):cache \
	--label "org.opencontainers.image.revision=$(REVISION)" \
	--label "org.opencontainers.image.created=$(TIME)" \
	$(DOCKER_SCCACHE) \
	--load \
	-f Dockerfile.plateau .

push-image: push-image-plateau

push-image-plateau:
	docker push $(PLATEAU_IMAGE):$(TAG)
