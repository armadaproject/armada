import { defineConfig } from "vitest/config"

export default defineConfig({
  test: {
    environment: "jsdom",
    include: ["**/*.test.tsx", "**/*.test.ts"],
    globals: true,
    setupFiles: ["./src/setupTests.ts"],
    outputFile: { junit: "./junit.xml" },
  },
})
