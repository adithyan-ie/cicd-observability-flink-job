# Jenkins controller image with the extra CLI tools pipeline stages shell out
# to: the Docker CLI (docker-outside-of-docker, talking to the host's
# docker.sock so `docker exec kcat ...` can reach the sibling kcat
# container — see docker-compose.yml) and python3 (used by pipeline stages
# to compact the CI/CD event JSON before it's handed to kcat).
FROM jenkins/jenkins:lts

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        gnupg \
        python3 \
    && install -m 0755 -d /etc/apt/keyrings \
    && curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg \
    && chmod a+r /etc/apt/keyrings/docker.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
        > /etc/apt/sources.list.d/docker.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends docker-ce-cli \
    && rm -rf /var/lib/apt/lists/*

USER jenkins
