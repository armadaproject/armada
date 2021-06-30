
{{- define "binoculars.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "binoculars.config.name" -}}
{{- printf "%s-%s" ( include "binoculars.name" .) "config" -}}
{{- end }}

{{- define "binoculars.config.filename" -}}
{{- printf "%s%s" ( include "binoculars.config.name" .) ".yaml" -}}
{{- end }}

{{- define "binoculars.users.name" -}}
{{- printf "%s-%s" ( include "binoculars.name" .) "users" -}}
{{- end }}

{{- define "binoculars.users.filename" -}}
{{- printf "%s%s" ( include "binoculars.users.name" .) ".yaml" -}}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "binoculars.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "binoculars.labels.identity" -}}
app: {{ include "binoculars.name" . }}
{{- end -}}

{{/*
Common labels
*/}}
{{- define "binoculars.labels.all" -}}
{{ include "binoculars.labels.identity" . }}
chart: {{ include "binoculars.chart" . }}
release: {{ .Release.Name }}
{{- if .Values.additionalLabels }}
{{ toYaml .Values.additionalLabels }}
{{- end }}
{{- end -}}
