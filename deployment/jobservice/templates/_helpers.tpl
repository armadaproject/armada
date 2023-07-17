
{{- define "jobservice.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "jobservice.config.name" -}}
{{- printf "%s-%s" ( include "jobservice.name" .) "config" -}}
{{- end }}

{{- define "jobservice.config.filename" -}}
{{- printf "%s%s" ( include "jobservice.config.name" .) ".yaml" -}}
{{- end }}

{{- define "jobservice.users.name" -}}
{{- printf "%s-%s" ( include "jobservice.name" .) "users" -}}
{{- end }}

{{- define "jobservice.users.filename" -}}
{{- printf "%s%s" ( include "jobservice.users.name" .) ".yaml" -}}
{{- end }}

{{- define "jobservice.resource-quantity" -}}
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

{{- define "jobservice.gomemlimit" -}}
    {{- with .Values.resources }}{{ with .limits }}{{ with .memory -}}
        {{- include "jobservice.resource-quantity" . | float64 | mulf 0.95 | ceil | int -}}
    {{- end }}{{ end }}{{ end -}}
{{- end -}}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "jobservice.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "jobservice.labels.identity" -}}
app: {{ include "jobservice.name" . }}
{{- end -}}

{{/*
Common labels
*/}}
{{- define "jobservice.labels.all" -}}
{{ include "jobservice.labels.identity" . }}
chart: {{ include "jobservice.chart" . }}
release: {{ .Release.Name }}
{{- if .Values.additionalLabels }}
{{ toYaml .Values.additionalLabels }}
{{- end }}
{{- end -}}
