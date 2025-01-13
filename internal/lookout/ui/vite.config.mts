import react from "@vitejs/plugin-react"
import { defineConfig, ProxyOptions } from "vite"

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
  server: {
    port: 3000,
    proxy: ["/api", "/config"].reduce<Record<string, ProxyOptions>>(
      (acc, path) => ({
        ...acc,
        [path]: {
          target: "http://localhost:10000",
        },
      }),
      {},
    ),
  },
  build: {
    outDir: "build",
  },
})
