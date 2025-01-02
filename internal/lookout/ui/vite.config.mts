import react from "@vitejs/plugin-react"
import { defineConfig, ProxyOptions } from "vite"

export default defineConfig({
  plugins: [react()],
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
