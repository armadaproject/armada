
{{- define "executor.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "executor.config.name" -}}
{{- printf "%s-%s" ( include "executor.name" .) "config" -}}
{{- end }}

{{- define "executor.application.config.filename" -}}
{{- printf "%s%s" ( include "executor.config.name" .) ".yaml" -}}
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
{{- end -}}


