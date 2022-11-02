
{{- define "jobservice.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "jobservice.config.name" -}}
{{- printf "%s-%s" ( include "jobservice.name" .) "config" -}}
{{- end }}

{{- define "jobservice.config.filename" -}}
{{- printf "%s%s" ( include "jobservice.config.name" .) ".yaml" -}}
{{- end }}

{{- define "jobservice.users.name" -}}
{{- printf "%s-%s" ( include "jobservice.name" .) "users" -}}
{{- end }}

{{- define "jobservice.users.filename" -}}
{{- printf "%s%s" ( include "jobservice.users.name" .) ".yaml" -}}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "jobservice.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "jobservice.labels.identity" -}}
app: {{ include "jobservice.name" . }}
{{- end -}}

{{/*
Common labels
*/}}
{{- define "jobservice.labels.all" -}}
{{ include "jobservice.labels.identity" . }}
chart: {{ include "jobservice.chart" . }}
release: {{ .Release.Name }}
{{- if .Values.additionalLabels }}
{{ toYaml .Values.additionalLabels }}
{{- end }}
{{- end -}}
