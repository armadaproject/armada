
{{- define "executor.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "executor.config.name" -}}
{{- printf "%s-%s" ( include "executor.name" .) "config" -}}
{{- end }}

{{- define "executor.config.filename" -}}
{{- printf "%s%s" ( include "executor.config.name" .) ".yaml" -}}
{{- end }}

{{- define "executor.api.credentials.name" -}}
{{- printf "%s-%s" ( include "executor.name" .) "credentials" -}}
{{- end }}

{{- define "executor.api.credentials.filename" -}}
{{- printf "%s%s" ( include "executor.api.credentials.name" .) ".yaml" -}}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "executor.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "executor.labels.identity" -}}
app: {{ include "executor.name" . }}
{{- end -}}

{{/*
Common labels
*/}}
{{- define "executor.labels.all" -}}
{{ include "executor.labels.identity" . }}
chart: {{ include "executor.chart" . }}
release: {{ .Release.Name }}
{{- if .Values.additionalLabels }}
{{ toYaml .Values.additionalLabels }}
{{- end }}
{{- end -}}
