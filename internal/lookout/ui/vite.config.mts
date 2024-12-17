import react from "@vitejs/plugin-react"
import { defineConfig, ProxyOptions } from "vite"
import path from "path"
import fs from "fs"
import { createRequire } from "node:module"

const require = createRequire(import.meta.url)

// See https://github.com/bvaughn/react-virtualized/issues/1722
const WRONG_CODE = `import { bpfrpt_proptype_WindowScroller } from "../WindowScroller.js";`

const reactVirtualized = () => {
  return {
    name: "my:react-virtualized",
    configResolved() {
      const file = require
        .resolve("react-virtualized")
        .replace(
          path.join("dist", "commonjs", "index.js"),
          path.join("dist", "es", "WindowScroller", "utils", "onScroll.js"),
        )
      const code = fs.readFileSync(file, "utf-8")
      const modified = code.replace(WRONG_CODE, "")
      fs.writeFileSync(file, modified)
    },
  }
}

export default defineConfig({
  plugins: [react(), reactVirtualized()],
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
})
