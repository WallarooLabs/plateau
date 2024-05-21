CONDUCTOR_LOCATION := $(dir $(realpath $(lastword $(MAKEFILE_LIST))))
GOOGLE_APPLICATION_CREDENTIALS ?= ""

DOCKER_SCCACHE := --build-context conductor=$(CONDUCTOR_LOCATION)

ifdef CI
GCS_CREDS ?= /run/secrets/gcs.json
DOCKER_SCCACHE := $(DOCKER_SCCACHE) \
	--build-arg GCS_CREDS=$(GCS_CREDS) \
    --secret id=gcs,src=$(GOOGLE_APPLICATION_CREDENTIALS)
else
GCS_CREDS ?= ""
DOCKER_SCCACHE := $(DOCKER_SCCACHE) \
		--build-arg GCS_CREDS=$(GCS_CREDS)
endif
