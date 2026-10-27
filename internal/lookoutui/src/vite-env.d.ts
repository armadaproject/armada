/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_APP_VERSION?: string
  readonly VITE_APP_COMMIT?: string
  readonly VITE_APP_BUILD_TIME?: string
}

interface ImportMeta {
  readonly env: ImportMetaEnv
}
