import react from "@vitejs/plugin-react"
import { defineConfig, ProxyOptions } from "vite"

const PROXY_PATHS = ["/api", "/config"]
const PROXY_OPTIONS: Record<string, string | ProxyOptions> = PROXY_PATHS.reduce<Record<string, ProxyOptions>>(
  (acc, path) => ({
    ...acc,
    [path]: {
      target: process.env.PROXY_TARGET || "http://localhost:10000",
      changeOrigin: true,
      secure: false,
    },
  }),
  {},
)

export default defineConfig({
  plugins: [
    react({
      jsxImportSource: "@emotion/react",
      babel: {
        plugins: [
          [
            "@emotion/babel-plugin",
            {
              importMap: {
                "@mui/system": {
                  styled: {
                    canonicalImport: ["@emotion/styled", "default"],
                    styledBaseImport: ["@mui/system", "styled"],
                  },
                },
                "@mui/material/styles": {
                  styled: {
                    canonicalImport: ["@emotion/styled", "default"],
                    styledBaseImport: ["@mui/material/styles", "styled"],
                  },
                },
                "@mui/material": {
                  styled: {
                    canonicalImport: ["@emotion/styled", "default"],
                    styledBaseImport: ["@mui/material", "styled"],
                  },
                },
              },
            },
          ],
        ],
      },
    }),
  ],
  preview: {
    port: 4173,
    proxy: PROXY_OPTIONS,
  },
  server: {
    port: 3000,
    proxy: PROXY_OPTIONS,
  },
  build: {
    outDir: "build",
  },
})
