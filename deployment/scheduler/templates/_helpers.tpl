{{- define "armada-scheduler.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "armada-scheduler.config.name" -}}
{{- printf "%s-%s" ( include "armada-scheduler.name" .) "config" -}}
{{- end }}

{{- define "armada-scheduler.config.filename" -}}
{{- printf "%s%s" ( include "armada-scheduler.config.name" .) ".yaml" -}}
{{- end }}

{{- define "armada-scheduler-ingester.config.filename" -}}
{{- printf "%s%s%s" ( include "armada-scheduler.config.name" .) "-ingester" ".yaml" -}}
{{- end }}

{{- define "armada-scheduler-ingester.serviceaccount.name" -}}
{{- printf "%s%s" ( include "armada-scheduler.name" .) "-ingester" -}}
{{- end }}


{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "armada-scheduler.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}



{{/*
Common labels
*/}}

{{- define "armada-scheduler.common-labels.all" -}}
chart: {{ include "armada-scheduler.chart" . }}
release: {{ .Release.Name }}
{{- if .Values.additionalLabels }}
{{ toYaml .Values.additionalLabels }}
{{- end }}
{{- end -}}

{{- define "armada-scheduler.labels.identity" -}}
app: {{ include "armada-scheduler.name" . }}
{{- end -}}

{{- define "armada-scheduler-ingester.labels.identity" -}}
app: {{ include "armada-scheduler.name" . }}-ingester
{{- end -}}

{{- define "armada-scheduler-pruner.labels.identity" -}}
app: {{ include "armada-scheduler.name" . }}-db-pruner
{{- end -}}

{{- define "armada-scheduler.labels.all" -}}
{{ include "armada-scheduler.common-labels.all" . }}
{{ include "armada-scheduler.labels.identity" . }}
{{- end -}}

{{- define "armada-scheduler-ingester.labels.all" -}}
{{ include "armada-scheduler.common-labels.all" . }}
{{ include "armada-scheduler-ingester.labels.identity" . }}
{{- end -}}

{{- define "armada-scheduler-pruner.labels.all" -}}
{{ include "armada-scheduler.common-labels.all" . }}
{{ include "armada-scheduler-pruner.labels.identity" . }}
{{- end -}}