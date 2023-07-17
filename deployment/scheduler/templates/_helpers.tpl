{{- define "armada-scheduler.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "armada-scheduler.config.name" -}}
{{- printf "%s-%s" ( include "armada-scheduler.name" .) "config" -}}
{{- end }}

{{- define "armada-scheduler-ingester.config.name" -}}
{{- printf "%s-%s-%s" ( include "armada-scheduler.name" .) "ingester" "config" -}}
{{- end }}

{{- define "armada-scheduler-pruner.config.name" -}}
{{- printf "%s-%s-%s" ( include "armada-scheduler.name" .) "pruner" "config" -}}
{{- end }}

{{- define "armada-scheduler.config.filename" -}}
{{- printf "%s%s" ( include "armada-scheduler.config.name" .) ".yaml" -}}
{{- end }}

{{- define "armada-scheduler-ingester.config.filename" -}}
{{- printf "%s%s%s" ( include "armada-scheduler.config.name" .) "-ingester" ".yaml" -}}
{{- end }}

{{- define "armada-scheduler-pruner.config.filename" -}}
{{- printf "%s%s%s" ( include "armada-scheduler.config.name" .) "-pruner" ".yaml" -}}
{{- end }}

{{- define "armada-scheduler-ingester.serviceaccount.name" -}}
{{- printf "%s%s" ( include "armada-scheduler.name" .) "-ingester" -}}
{{- end }}

{{- define "armada-scheduler-ingester.resource-quantity" -}}
    {{- $value := . -}}
    {{- $unit := 1.0 -}}
    {{- if typeIs "string" . -}}
        {{- $base2 := dict "Ki" 0x1p10 "Mi" 0x1p20 "Gi" 0x1p30 "Ti" 0x1p40 "Pi" 0x1p50 "Ei" 0x1p60 -}}
        {{- $base10 := dict "m" 1e-3 "k" 1e3 "M" 1e6 "G" 1e9 "T" 1e12 "P" 1e15 "E" 1e18 -}}
        {{- range $k, $v := merge $base2 $base10 -}}
            {{- if hasSuffix $k $ -}}
                {{- $value = trimSuffix $k $ -}}
                {{- $unit = $v -}}
            {{- end -}}
        {{- end -}}
    {{- end -}}
    {{- mulf (float64 $value) $unit -}}
{{- end -}}

{{- define "armada-scheduler-ingester.gomemlimit" -}}
    {{- with .Values.ingester.resources }}{{ with .limits }}{{ with .memory -}}
        {{- include "armada-scheduler-ingester.resource-quantity" . | float64 | mulf 0.95 | ceil | int -}}
    {{- end }}{{ end }}{{ end -}}
{{- end -}}

{{- define "armada-scheduler.gomemlimit" -}}
    {{- with .Values.scheduler.resources }}{{ with .limits }}{{ with .memory -}}
        {{- include "armada-scheduler-ingester.resource-quantity" . | float64 | mulf 0.95 | ceil | int -}}
    {{- end }}{{ end }}{{ end -}}
{{- end -}}


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