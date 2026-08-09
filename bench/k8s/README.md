# Running batch-load in-cluster

Running through `kubectl port-forward` causes spurious
`Failed to buffer the request body` (400) errors: the tunnel is a single
apiserver-proxied stream that stalls under sustained load, truncating request
bodies. Running in-cluster talks straight to the Service and avoids this.

## Build and push the image (outside CI)

The `batch-load` target in the repo-root `Dockerfile` produces a static musl
binary in a `scratch` image. Build it directly with buildx — no CI required:

```sh
# from the repo root
REGISTRY=ghcr.io/wallaroolabs/plateau-bench
TAG=dev

docker buildx build \
  --target batch-load \
  --platform linux/amd64 \
  --build-arg RUST_VERSION=1.84.1 \
  -t $REGISTRY:$TAG \
  --push \
  .
```

Notes:
- `--target batch-load` selects the bench stage (the default target builds the
  plateau server).
- `--platform linux/amd64` matches a typical cluster; add/replace with
  `linux/arm64` if your nodes are arm.
- `--push` uploads straight to the registry. Log in first
  (`docker login ghcr.io`). Use any registry your cluster can pull from.

## Deploy the Job

Edit `job.yaml`:
- `image:` → the tag you just pushed
- `PLATEAU_URL` → `http://<plateau-service>.<namespace>.svc.cluster.local:3030`

Then:

```sh
kubectl apply -n <namespace> -f bench/k8s/job.yaml
kubectl logs -n <namespace> -f job/batch-load
```

The schemas dir and state file live on a PVC, so the Job resumes the same
topic pool and schedule if it restarts. For a throwaway run, replace the
`persistentVolumeClaim` volume with `emptyDir: {}`.

To restart with a clean slate, delete the PVC (`kubectl delete pvc
batch-load-data`) before re-applying.
