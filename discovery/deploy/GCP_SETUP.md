# Running Trove on a GCP Always Free e2-micro VM

The discovery server is tiny and stateless except for a small SQLite analytics
file and its persistent key/cert. It fits comfortably in the GCP Always Free
tier. **No peer file data ever flows through this server**, so egress is
negligible and stays well inside the free allowance.

The server terminates TLS itself with a self-signed Ed25519 certificate and
authenticates clients by mutual TLS — **no domain, no Let's Encrypt, no reverse
proxy.** Trust is anchored to the server's **fingerprint**, which it prints on
first boot.

## 1. Create the VM (free tier)

Only an **e2-micro in `us-west1`, `us-central1`, or `us-east1`** qualifies for
the Always Free tier. Anything larger, or in another region, will be billed.

```sh
gcloud compute instances create trove \
  --zone=us-central1-a \
  --machine-type=e2-micro \
  --image-family=debian-12 --image-project=debian-cloud \
  --boot-disk-type=pd-standard --boot-disk-size=30GB \
  --no-service-account --no-scopes
```

- **Machine type:** `e2-micro` (1 shared vCPU, 1 GB RAM) — the only free type.
- **Disk:** Standard persistent disk, **30 GB** (the free maximum). Do not use SSD.
- **Ops Agent:** do **not** install it; it adds memory pressure on a 1 GB VM.

## 2. Firewall: allow only 443

```sh
gcloud compute firewall-rules create trove-tls \
  --allow=tcp:443 --direction=INGRESS --target-tags=trove
gcloud compute instances add-tags trove --zone=us-central1-a --tags=trove
```

Do **not** open the metrics port (9090). It is published only to the VM's
loopback; reach it through an SSH tunnel (step 5).

## 3. Reserve a static external IP (optional)

A static IP is convenient but not the trust anchor — the **fingerprint** is. If
the IP changes you only redistribute the new address; pins keep working.

```sh
gcloud compute addresses create trove-ip --region=us-central1
gcloud compute instances delete-access-config trove --zone=us-central1-a \
  --access-config-name="external-nat"
gcloud compute instances add-access-config trove --zone=us-central1-a \
  --access-config-name="external-nat" --address=trove-ip
```

> Cost safety: a static IP is free **only while attached to a running VM**. An
> unattached (or attached-to-stopped) reserved IP is billed hourly. If you tear
> the VM down, release the address: `gcloud compute addresses delete trove-ip`.

## 4. Install Docker and start the server

```sh
gcloud compute ssh trove --zone=us-central1-a

# On the VM:
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker "$USER" && newgrp docker

git clone https://github.com/GhentiLabs/Trove.git
cd Trove/discovery/deploy
docker compose up -d --build
```

The key and certificate are generated on first boot and persisted in the `data`
volume, so the fingerprint is stable across restarts and rebuilds. Read it from
the logs and distribute the connection string to clients:

```sh
docker compose logs discovery-server | grep fingerprint
# ..."fingerprint":"<52-char id>","connect":"trove://0.0.0.0:8443?id=<id>"
```

Hand clients `trove://<vm-public-ip>:443?id=<fingerprint>`. The client pins that
fingerprint; the IP is just an address. The server cannot know the `443 -> 8443`
publish mapping, so set `TROVE_DISCOVERY_ADVERTISE_ADDR=<vm-public-ip>:443` (in
the compose `environment:`) to have it log the exact client-facing string.

## 5. Viewing metrics and health (operators only)

The metrics and `/healthz` endpoints expose operational detail and are never
served publicly. Reach them over an SSH tunnel from your workstation:

```sh
gcloud compute ssh trove --zone=us-central1-a -- -L 9090:127.0.0.1:9090
# then locally:
curl http://127.0.0.1:9090/healthz
curl http://127.0.0.1:9090/metrics
```

## Cost-safety checklist

- Only **e2-micro** in a free-tier region is free; double-check the type/region.
- Don't leave a **reserved static IP unattached** — release it when not in use.
- Keep the disk **Standard 30 GB**; SSD or larger disks are billed.
- The analytics disk-usage cap (`TROVE_DISCOVERY_ANALYTICS_DISK_CAP_BYTES`, default 256 MB)
  stops ingestion before the disk fills — adjust it to your disk headroom.
