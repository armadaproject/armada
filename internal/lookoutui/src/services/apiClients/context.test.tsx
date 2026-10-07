import { renderHook } from "@testing-library/react"

import type { Config } from "../../config"

import { ApiClientsProvider, useApiClients } from "./context"

const mockConfig = vi.hoisted(() => ({ current: {} as Config }))

vi.mock("../../config", () => ({ getConfig: () => mockConfig.current }))

const makeConfig = (overrides: Partial<Config>): Config =>
  ({
    armadaApiBaseUrl: "http://armada.example.com",
    binocularsBaseUrlPattern: "",
    binocularsStaticBaseUrls: {},
    ...overrides,
  }) as Config

// The base URL a Binoculars client was built with. basePath is protected on the generated client's configuration,
// and the URL is the only thing getBinocularsApi decides, so it is read directly.
const basePathOf = (api: unknown): string => (api as { configuration: { basePath: string } }).configuration.basePath

describe("ApiClientsProvider getBinocularsApi", () => {
  const renderApiClients = () => renderHook(() => useApiClients(), { wrapper: ApiClientsProvider })

  it("uses a cluster's static URL, ahead of the pattern", () => {
    mockConfig.current = makeConfig({
      binocularsBaseUrlPattern: "https://{CLUSTER_ID}.example.com",
      binocularsStaticBaseUrls: { "cluster-1": "http://localhost:8084", "cluster-2": "http://localhost:8094" },
    })
    const { result } = renderApiClients()

    expect(basePathOf(result.current.getBinocularsApi("cluster-1"))).toBe("http://localhost:8084")
    expect(basePathOf(result.current.getBinocularsApi("cluster-2"))).toBe("http://localhost:8094")
  })

  it("matches static URL keys and cluster IDs case-insensitively", () => {
    // The Lookout server's config loader lower-cases map keys, so a cluster ID like "Cluster1" arrives as "cluster1".
    mockConfig.current = makeConfig({
      binocularsBaseUrlPattern: "https://{CLUSTER_ID}.example.com",
      binocularsStaticBaseUrls: { cluster1: "http://localhost:8084", CLUSTER2: "http://localhost:8094" },
    })
    const { result } = renderApiClients()

    expect(basePathOf(result.current.getBinocularsApi("Cluster1"))).toBe("http://localhost:8084")
    expect(basePathOf(result.current.getBinocularsApi("cluster2"))).toBe("http://localhost:8094")
  })

  it("uses the pattern, with {CLUSTER_ID} replaced, for a cluster with no static URL", () => {
    mockConfig.current = makeConfig({
      binocularsBaseUrlPattern: "https://{CLUSTER_ID}.example.com",
      binocularsStaticBaseUrls: { "cluster-1": "http://localhost:8084" },
    })
    const { result } = renderApiClients()

    expect(basePathOf(result.current.getBinocularsApi("cluster-9"))).toBe("https://cluster-9.example.com")
  })

  it("uses the pattern as is when there are no static URLs", () => {
    mockConfig.current = makeConfig({ binocularsBaseUrlPattern: "http://localhost:8084" })
    const { result } = renderApiClients()

    expect(basePathOf(result.current.getBinocularsApi("cluster-1"))).toBe("http://localhost:8084")
  })

  it("reuses the cached client for a cluster while the config is unchanged", () => {
    mockConfig.current = makeConfig({ binocularsStaticBaseUrls: { "cluster-1": "http://localhost:8084" } })
    const { result, rerender } = renderApiClients()

    const first = result.current.getBinocularsApi("cluster-1")
    rerender()

    expect(result.current.getBinocularsApi("cluster-1")).toBe(first)
  })

  it("rebuilds the cached client when the static URLs change", () => {
    mockConfig.current = makeConfig({ binocularsStaticBaseUrls: { "cluster-1": "http://localhost:8084" } })
    const { result, rerender } = renderApiClients()

    const before = result.current.getBinocularsApi("cluster-1")
    expect(basePathOf(before)).toBe("http://localhost:8084")

    mockConfig.current = makeConfig({ binocularsStaticBaseUrls: { "cluster-1": "http://localhost:9999" } })
    rerender()

    const after = result.current.getBinocularsApi("cluster-1")
    expect(after).not.toBe(before)
    expect(basePathOf(after)).toBe("http://localhost:9999")
  })
})
