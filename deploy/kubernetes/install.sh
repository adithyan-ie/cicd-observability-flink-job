#!/usr/bin/env bash
set -euo pipefail

FLINK_OPERATOR_VERSION=1.15.0

sudo dnf update -y
sudo dnf install -y docker git gettext
sudo systemctl enable docker
sudo systemctl start docker
sudo usermod -aG docker ec2-user

curl -sfL https://get.k3s.io | sh -
sudo systemctl enable k3s
sudo systemctl start k3s

mkdir -p /home/ec2-user/.kube
sudo cp /etc/rancher/k3s/k3s.yaml /home/ec2-user/.kube/config
sudo chown ec2-user:ec2-user /home/ec2-user/.kube/config
sudo chmod 600 /home/ec2-user/.kube/config

curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash

export KUBECONFIG=/home/ec2-user/.kube/config
helm repo add flink-operator-repo https://downloads.apache.org/flink/flink-kubernetes-operator-${FLINK_OPERATOR_VERSION}/
helm repo update
# webhook.create=false: the chart's admission webhook needs cert-manager (for
# its self-signed Certificate/Issuer) which isn't installed on this cluster.
# The webhook only validates FlinkDeployment specs at admission time — not
# required for the operator or job to run.
helm install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator \
  --set webhook.create=false
