
{{- define "armada-scheduler.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "armada-scheduler.config.name" -}}
{{- printf "%s-%s" ( include "armada-scheduler.name" .) "config" -}}
{{- end }}

{{- define "armada-scheduler.config.filename" -}}
{{- printf "%s%s" ( include "armada-scheduler.config.name" .) ".yaml" -}}
{{- end }}

{{- define "armada-scheduler.users.name" -}}
{{- printf "%s-%s" ( include "armada-scheduler.name" .) "users" -}}
{{- end }}

{{- define "armada-scheduler.users.filename" -}}
{{- printf "%s%s" ( include "armada-scheduler.users.name" .) ".yaml" -}}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "armada-scheduler.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "armada-scheduler.labels.identity" -}}
app: {{ include "armada-scheduler.name" . }}
{{- end -}}

{{/*
Common labels
*/}}
{{- define "armada-scheduler.labels.all" -}}
{{ include "armada-scheduler.labels.identity" . }}
chart: {{ include "armada-scheduler.chart" . }}
release: {{ .Release.Name }}
{{- if .Values.additionalLabels }}
{{ toYaml .Values.additionalLabels }}
{{- end }}
{{- end -}}
