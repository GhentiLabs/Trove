#!/usr/bin/env bash
# Build the discovery-server image off-box (the e2-micro can't compile it) and
# ship it to the GCE VM, reusing the persisted key volume so the fingerprint is
# stable across deploys. Override VM/ZONE/IMAGE via env.
set -euo pipefail

VM="${VM:-free-tier-vm}"
ZONE="${ZONE:-us-central1-a}"
IMAGE="${IMAGE:-trove-discovery-server:latest}"
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

echo ">> building $IMAGE (linux/amd64)"
docker build --platform linux/amd64 --provenance=false \
  -f "$ROOT/discovery/deploy/Dockerfile" -t "$IMAGE" "$ROOT"

img="$(mktemp -t trove-img).tgz"
trap 'rm -f "$img"' EXIT
echo ">> saving image"
docker save "$IMAGE" | gzip >"$img"

echo ">> shipping to $VM ($ZONE)"
gcloud compute scp "$img" "$VM:~/trove-img.tgz" --zone "$ZONE" --quiet
gcloud compute scp "$ROOT/discovery/deploy/docker-compose.yml" "$VM:~/docker-compose.yml" --zone "$ZONE" --quiet

echo ">> loading + restarting (project 'deploy' keeps the existing key volume)"
gcloud compute ssh "$VM" --zone "$ZONE" --quiet --command="\
  sudo docker load -i ~/trove-img.tgz && \
  sudo docker compose -p deploy -f ~/docker-compose.yml up -d --no-build && \
  sudo docker image prune -f >/dev/null && \
  sudo docker compose -p deploy -f ~/docker-compose.yml logs --tail=3 discovery-server"

echo ">> done"
