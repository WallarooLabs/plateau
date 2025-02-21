.PHONY: default image push-image image-plateau push-image-plateau check-fmt fmt lint clean test

TAG                      ?= $(shell git rev-parse --short  HEAD)
IMAGE_ROOT                = ghcr.io/wallaroolabs
PLATEAU_IMAGE  			  = $(IMAGE_ROOT)/plateau
DOCKER                    = docker buildx
REVISION                  = $(shell git describe --match="" --always --abbrev=40 --dirty)
TIME                      = $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
RUST_VERSION              = 1.84.1

test:
	cargo test --workspace --features batch,polars,replicate -- --nocapture

fmt: check-fmt

check-fmt:
	cargo fmt --all -- --check

lint:
	cargo clippy --workspace --all-targets --features batch,polars,replicate -- -D warnings

coverage-%:
	@$(MAKE) -C upstream -f ../../rust-grcov.mk $@

clean:
	cargo clean

image: image-plateau

# include sccache.mk

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
	--progress plain \
	-f Dockerfile .

push-image: push-image-plateau

push-image-plateau:
	docker push $(PLATEAU_IMAGE):$(TAG)
