
{{- define "armada.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "armada.config.name" -}}
{{- printf "%s-%s" ( include "armada.name" .) "config" -}}
{{- end }}

{{- define "armada.config.filename" -}}
{{- printf "%s%s" ( include "armada.config.name" .) ".yaml" -}}
{{- end }}

{{- define "armada.users.name" -}}
{{- printf "%s-%s" ( include "armada.name" .) "users" -}}
{{- end }}

{{- define "armada.users.filename" -}}
{{- printf "%s%s" ( include "armada.users.name" .) ".yaml" -}}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "armada.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "armada.labels.identity" -}}
app: {{ include "armada.name" . }}
{{- end -}}

{{/*
Common labels
*/}}
{{- define "armada.labels.all" -}}
{{ include "armada.labels.identity" . }}
chart: {{ include "armada.chart" . }}
release: {{ .Release.Name }}
{{- if .Values.additionalLabels }}
{{ toYaml .Values.additionalLabels }}
{{- end }}
{{- end -}}
