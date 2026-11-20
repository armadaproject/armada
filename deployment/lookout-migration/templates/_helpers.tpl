
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

{{/*
Pod template shared by the migration and partition Jobs. Takes a dict with
"root" (the chart context), "args" (the lookout arguments before --config)
and "resources".
*/}}
{{- define "lookout.migration.podTemplate" -}}
metadata:
  name: {{ include "lookout.name" .root }}
  labels:
    {{- include "lookout.labels.all" .root | nindent 4 }}
spec:
  restartPolicy: Never
  serviceAccountName: {{ .root.Values.customServiceAccount | default (include "lookout.name" .root) }}
  securityContext:
    runAsUser: 1000
    runAsGroup: 2000
  {{- if .root.Values.tolerations }}
  tolerations:
    {{- toYaml .root.Values.tolerations | nindent 4 }}
  {{- end }}
  containers:
    - name: lookout
      imagePullPolicy: IfNotPresent
      image: {{ .root.Values.image.repository }}:{{ required "A value is required for .Values.image.tag" .root.Values.image.tag }}
      args:
        {{- range .args }}
        - {{ . }}
        {{- end }}
        - --config
        - /config/application_config.yaml
      {{- if .root.Values.env }}
      env:
        {{- toYaml .root.Values.env | nindent 8 -}}
      {{- end }}
      resources:
        {{- toYaml .resources | nindent 8 }}
      volumeMounts:
        - name: user-config
          mountPath: /config/application_config.yaml
          subPath: {{ include "lookout.config.filename" .root }}
          readOnly: true
        {{- if .root.Values.additionalVolumeMounts }}
        {{- toYaml .root.Values.additionalVolumeMounts | nindent 8 -}}
        {{- end }}
      securityContext:
        allowPrivilegeEscalation: false
  volumes:
    - name: user-config
      configMap:
        name: {{ include "lookout.config.name" .root }}
    {{- if .root.Values.additionalVolumes }}
    {{- toYaml .root.Values.additionalVolumes | nindent 4 }}
    {{- end }}
{{- end -}}
