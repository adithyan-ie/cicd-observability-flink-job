#!/usr/bin/env bash
set -euo pipefail

sudo dnf update -y
sudo dnf install -y docker git
sudo systemctl enable docker
sudo systemctl start docker
sudo usermod -aG docker ec2-user
sudo mkdir -p /usr/local/lib/docker/cli-plugins

# curl -o against this instance's disk has been observed to intermittently
# fail mid-write (CURLE_WRITE_ERROR) on an otherwise-complete download —
# retry a few times rather than letting one flaky write kill the whole
# script (--retry doesn't cover this: it only retries transient network/HTTP
# errors, not local write failures).
download_with_retry() {
  local url="$1" dest="$2" attempt
  for attempt in 1 2 3 4 5; do
    if sudo curl -fSL "$url" -o "$dest"; then
      return 0
    fi
    echo "download of $url failed (attempt $attempt/5), retrying..." >&2
    sleep 2
  done
  echo "giving up on $url after 5 attempts" >&2
  return 1
}

download_with_retry https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64 /usr/local/lib/docker/cli-plugins/docker-compose
sudo chmod +x /usr/local/lib/docker/cli-plugins/docker-compose

# buildx — Amazon Linux's docker package doesn't ship it, but `docker compose
# up --build` (needed here since the jenkins service now builds a custom
# image, see ./Dockerfile) requires it.
BUILDX_VERSION=$(curl -fsSL https://api.github.com/repos/docker/buildx/releases/latest | grep -m1 '"tag_name"' | cut -d '"' -f4)
download_with_retry "https://github.com/docker/buildx/releases/download/${BUILDX_VERSION}/buildx-${BUILDX_VERSION}.linux-amd64" /usr/local/lib/docker/cli-plugins/docker-buildx
sudo chmod +x /usr/local/lib/docker/cli-plugins/docker-buildx
