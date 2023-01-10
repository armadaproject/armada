
{{- define "lookout_ingester.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "lookout_ingester.config.name" -}}
{{- printf "%s-%s" ( include "lookout_ingester.name" .) "config" -}}
{{- end }}

{{- define "lookout_ingester.config.filename" -}}
{{- printf "%s%s" ( include "lookout_ingester.config.name" .) ".yaml" -}}
{{- end }}

{{- define "lookout_ingester.users.name" -}}
{{- printf "%s-%s" ( include "lookout_ingester.name" .) "users" -}}
{{- end }}

{{- define "lookout_ingester.users.filename" -}}
{{- printf "%s%s" ( include "lookout_ingester.users.name" .) ".yaml" -}}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "lookout_ingester.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "lookout_ingester.labels.identity" -}}
app: {{ include "lookout_ingester.name" . }}
{{- end -}}

{{/*
Common labels
*/}}
{{- define "lookout_ingester.labels.all" -}}
{{ include "lookout_ingester.labels.identity" . }}
chart: {{ include "lookout_ingester.chart" . }}
release: {{ .Release.Name }}
{{- if .Values.additionalLabels }}
{{ toYaml .Values.additionalLabels }}
{{- end }}
{{- end -}}
