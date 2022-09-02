
{{- define "event_ingester.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "event_ingester.config.name" -}}
{{- printf "%s-%s" ( include "event_ingester.name" .) "config" -}}
{{- end }}

{{- define "event_ingester.config.filename" -}}
{{- printf "%s%s" ( include "event_ingester.config.name" .) ".yaml" -}}
{{- end }}

{{- define "event_ingester.users.name" -}}
{{- printf "%s-%s" ( include "event_ingester.name" .) "users" -}}
{{- end }}

{{- define "event_ingester.users.filename" -}}
{{- printf "%s%s" ( include "event_ingester.users.name" .) ".yaml" -}}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "event_ingester.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "event_ingester.labels.identity" -}}
app: {{ include "event_ingester.name" . }}
{{- end -}}

{{/*
Common labels
*/}}
{{- define "event_ingester.labels.all" -}}
{{ include "event_ingester.labels.identity" . }}
chart: {{ include "event_ingester.chart" . }}
release: {{ .Release.Name }}
{{- if .Values.additionalLabels }}
{{ toYaml .Values.additionalLabels }}
{{- end }}
{{- end -}}
