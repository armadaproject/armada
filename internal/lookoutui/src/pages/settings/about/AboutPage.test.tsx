import { QueryClient, QueryClientProvider } from "@tanstack/react-query"
import { render, screen, waitFor } from "@testing-library/react"
import { http, HttpResponse } from "msw"
import { setupServer } from "msw/node"
import { MemoryRouter } from "react-router-dom"

import { AboutPage } from "./AboutPage"

const server = setupServer()

const renderAboutPage = () => {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  })
  return render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter>
        <AboutPage />
      </MemoryRouter>
    </QueryClientProvider>,
  )
}

describe("AboutPage", () => {
  beforeAll(() => {
    server.listen()
  })

  afterEach(() => {
    server.resetHandlers()
    // cspell:ignore unstub
    vi.unstubAllEnvs()
  })

  afterAll(() => {
    server.close()
  })

  it("renders both versions and links to GitHub releases when both are populated", async () => {
    vi.stubEnv("VITE_APP_VERSION", "v1.2.3")
    vi.stubEnv("VITE_APP_COMMIT", "abc1234")
    vi.stubEnv("VITE_APP_BUILD_TIME", "2026-05-14T10:30:00Z")

    server.use(
      http.get("/api/v1/version", () =>
        HttpResponse.json({
          version: "v1.2.3",
          commit: "abc1234",
          buildTime: "2026-05-14T10:30:00Z",
        }),
      ),
    )

    renderAboutPage()

    await waitFor(() => {
      expect(screen.getAllByRole("link", { name: "v1.2.3" })).toHaveLength(2)
    })
    screen.getAllByRole("link", { name: "v1.2.3" }).forEach((link) => {
      expect(link).toHaveAttribute("href", "https://github.com/armadaproject/armada/releases/tag/v1.2.3")
    })

    expect(screen.queryByLabelText("Frontend and backend versions differ")).not.toBeInTheDocument()
  })

  it("shows an error alert and falls back to 'unknown' when the backend fetch fails", async () => {
    vi.stubEnv("VITE_APP_VERSION", "v1.2.3")
    vi.stubEnv("VITE_APP_COMMIT", "abc1234")
    vi.stubEnv("VITE_APP_BUILD_TIME", "2026-05-14T10:30:00Z")

    server.use(http.get("/api/v1/version", () => HttpResponse.error()))

    renderAboutPage()

    await waitFor(() => {
      expect(screen.getByText(/Could not load the backend version/)).toBeInTheDocument()
    })

    expect(screen.getAllByText(/commit unknown/).length).toBeGreaterThan(0)
    expect(screen.queryByLabelText("Frontend and backend versions differ")).not.toBeInTheDocument()
  })

  it("shows an error alert when the backend returns a non-OK HTTP status", async () => {
    vi.stubEnv("VITE_APP_VERSION", "v1.2.3")
    vi.stubEnv("VITE_APP_COMMIT", "abc1234")
    vi.stubEnv("VITE_APP_BUILD_TIME", "2026-05-14T10:30:00Z")

    server.use(
      http.get("/api/v1/version", () => HttpResponse.json({ title: "internal server error" }, { status: 500 })),
    )

    renderAboutPage()

    await waitFor(() => {
      expect(screen.getByText(/Could not load the backend version/)).toBeInTheDocument()
    })

    expect(screen.getAllByText(/commit unknown/).length).toBeGreaterThan(0)
  })

  it("renders 'dev' as plain text without a release link", async () => {
    vi.stubEnv("VITE_APP_VERSION", "dev")
    vi.stubEnv("VITE_APP_COMMIT", "unknown")
    vi.stubEnv("VITE_APP_BUILD_TIME", "unknown")

    server.use(
      http.get("/api/v1/version", () => HttpResponse.json({ version: "dev", commit: "unknown", buildTime: "unknown" })),
    )

    renderAboutPage()

    await waitFor(() => {
      expect(screen.getAllByText("dev").length).toBeGreaterThan(0)
    })
    expect(screen.queryByRole("link", { name: "dev" })).not.toBeInTheDocument()
  })

  it("shows the mismatched-versions warning when frontend and backend release versions differ", async () => {
    vi.stubEnv("VITE_APP_VERSION", "v1.2.3")
    vi.stubEnv("VITE_APP_COMMIT", "abc1234")
    vi.stubEnv("VITE_APP_BUILD_TIME", "2026-05-14T10:30:00Z")

    server.use(
      http.get("/api/v1/version", () =>
        HttpResponse.json({
          version: "v1.2.4",
          commit: "def5678",
          buildTime: "2026-05-15T10:30:00Z",
        }),
      ),
    )

    renderAboutPage()

    await waitFor(() => {
      expect(screen.getByLabelText("Frontend and backend versions differ")).toBeInTheDocument()
    })
  })
})
