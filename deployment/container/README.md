# AeternumDB Containerized Deployment

Deploy AeternumDB using Docker and Kubernetes.

## Docker

### Build

```bash
docker build -t aeternumdb:latest -f Dockerfile ../..
```

### Run

```bash
docker run -p 5432:5432 aeternumdb:latest
```

## Kubernetes

### Deploy

```bash
kubectl apply -f kubernetes.yaml
```

### Access

```bash
kubectl port-forward -n aeternumdb svc/aeternumdb 5432:5432
```

### Scale

```bash
kubectl scale deployment/aeternumdb -n aeternumdb --replicas=5
```
