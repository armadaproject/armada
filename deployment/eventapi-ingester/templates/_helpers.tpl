
{{- define "eventapi_ingester.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "eventapi_ingester.config.name" -}}
{{- printf "%s-%s" ( include "eventapi_ingester.name" .) "config" -}}
{{- end }}

{{- define "eventapi_ingester.config.filename" -}}
{{- printf "%s%s" ( include "eventapi_ingester.config.name" .) ".yaml" -}}
{{- end }}

{{- define "eventapi_ingester.users.name" -}}
{{- printf "%s-%s" ( include "eventapi_ingester.name" .) "users" -}}
{{- end }}

{{- define "eventapi_ingester.users.filename" -}}
{{- printf "%s%s" ( include "eventapi_ingester.users.name" .) ".yaml" -}}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "eventapi_ingester.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "eventapi_ingester.labels.identity" -}}
app: {{ include "eventapi_ingester.name" . }}
{{- end -}}

{{/*
Common labels
*/}}
{{- define "eventapi_ingester.labels.all" -}}
{{ include "eventapi_ingester.labels.identity" . }}
chart: {{ include "eventapi_ingester.chart" . }}
release: {{ .Release.Name }}
{{- if .Values.additionalLabels }}
{{ toYaml .Values.additionalLabels }}
{{- end }}
{{- end -}}
