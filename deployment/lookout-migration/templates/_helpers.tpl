
{{- define "lookout.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "lookout.config.name" -}}
{{- printf "%s-%s" ( include "lookout.name" .) "config" -}}
{{- end }}

{{- define "lookout.config.filename" -}}
{{- printf "%s%s" ( include "lookout.config.name" .) ".yaml" -}}
{{- end }}

{{- define "lookout.users.name" -}}
{{- printf "%s-%s" ( include "lookout.name" .) "users" -}}
{{- end }}

{{- define "lookout.users.filename" -}}
{{- printf "%s%s" ( include "lookout.users.name" .) ".yaml" -}}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "lookout.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "lookout.labels.identity" -}}
app: {{ include "lookout.name" . }}
{{- end -}}

{{/*
Common labels
*/}}
{{- define "lookout.labels.all" -}}
{{ include "lookout.labels.identity" . }}
chart: {{ include "lookout.chart" . }}
release: {{ .Release.Name }}
{{- if .Values.additionalLabels }}
{{ toYaml .Values.additionalLabels }}
{{- end }}
{{- end -}}
