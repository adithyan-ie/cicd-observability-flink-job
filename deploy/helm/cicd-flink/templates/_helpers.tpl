{{/*
Expand the name of the chart.
*/}}
{{- define "cicd-flink.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Full name — release + chart name
*/}}
{{- define "cicd-flink.fullname" -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "cicd-flink.labels" -}}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{ include "cicd-flink.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "cicd-flink.selectorLabels" -}}
app.kubernetes.io/name: {{ include "cicd-flink.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}
