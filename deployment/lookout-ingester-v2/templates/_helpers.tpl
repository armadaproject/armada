
{{- define "lookout_ingester_v2.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "lookout_ingester_v2.config.name" -}}
{{- printf "%s-%s" ( include "lookout_ingester_v2.name" .) "config" -}}
{{- end }}

{{- define "lookout_ingester_v2.config.filename" -}}
{{- printf "%s%s" ( include "lookout_ingester_v2.config.name" .) ".yaml" -}}
{{- end }}

{{- define "lookout_ingester_v2.users.name" -}}
{{- printf "%s-%s" ( include "lookout_ingester_v2.name" .) "users" -}}
{{- end }}

{{- define "lookout_ingester_v2.users.filename" -}}
{{- printf "%s%s" ( include "lookout_ingester_v2.users.name" .) ".yaml" -}}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "lookout_ingester_v2.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "lookout_ingester_v2.labels.identity" -}}
app: {{ include "lookout_ingester_v2.name" . }}
{{- end -}}

{{/*
Common labels
*/}}
{{- define "lookout_ingester_v2.labels.all" -}}
{{ include "lookout_ingester_v2.labels.identity" . }}
chart: {{ include "lookout_ingester_v2.chart" . }}
release: {{ .Release.Name }}
{{- if .Values.additionalLabels }}
{{ toYaml .Values.additionalLabels }}
{{- end }}
{{- end -}}
