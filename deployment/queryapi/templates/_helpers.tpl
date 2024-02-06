
{{- define "queryapi.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "queryapi.config.name" -}}
{{- printf "%s-%s" ( include "queryapi.name" .) "config" -}}
{{- end }}

{{- define "queryapi.config.filename" -}}
{{- printf "%s%s" ( include "queryapi.config.name" .) ".yaml" -}}
{{- end }}

{{- define "queryapi.users.name" -}}
{{- printf "%s-%s" ( include "queryapi.name" .) "users" -}}
{{- end }}

{{- define "queryapi.users.filename" -}}
{{- printf "%s%s" ( include "queryapi.users.name" .) ".yaml" -}}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "queryapi.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "queryapi.labels.identity" -}}
app: {{ include "queryapi.name" . }}
{{- end -}}

{{/*
Common labels
*/}}
{{- define "queryapi.labels.all" -}}
{{ include "queryapi.labels.identity" . }}
chart: {{ include "queryapi.chart" . }}
release: {{ .Release.Name }}
{{- if .Values.additionalLabels }}
{{ toYaml .Values.additionalLabels }}
{{- end }}
{{- end -}}
