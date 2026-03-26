# rustream production template (minimal skeleton)

Quick ways to try rustream end-to-end without Kafka:

## Local (docker-compose)
```
cd examples/production-template
cp config.example.yaml config.yaml   # edit Postgres/S3/MinIO creds if needed
docker compose up --build
# worker + status API come up; status UI on http://localhost:8080/jobs/html
```

Services:
- Postgres (demo/demo)
- MinIO (admin/admin) at http://localhost:9001
- rustream worker (polls jobs table)
- rustream status API (port 8080)

## Kubernetes (Helm skeleton)
```
cd examples/production-template/helm
helm install rustream . \
  --set controlDbUrl=postgres://user:pass@host:5432/db \
  --set image.repository=ghcr.io/yourorg/rustream \
  --set image.tag=latest
```
Notes:
- Chart is minimal: no Secrets/IAM wired yet. Add IRSA/kiam annotations and env for AWS creds.
- Mount your `config.yaml` via a ConfigMap/Secret and add a bootstrap Job that runs `rustream add-job`.

## AWS Terraform stub
```
cd examples/production-template/terraform
terraform init
terraform apply -var bucket_name=rustream-demo-bucket
```
Creates S3 bucket + IAM role/policy for ECS tasks. You still need to wire ECS service/task defs to run rustream.

## GHCR Docker image
A GitHub Actions workflow builds/pushes `ghcr.io/<owner>/rustream:latest` on tag pushes (`.github/workflows/publish-docker.yml`). Customize tags as needed.
