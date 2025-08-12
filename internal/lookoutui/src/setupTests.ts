// jest-dom adds custom jest matchers for asserting on DOM nodes.
// allows you to do things like:
// expect(element).toHaveTextContent(/react/i)
// learn more: https://github.com/testing-library/jest-dom
import "@testing-library/jest-dom"

import { DEFAULT_LOOKOUT_UI_CONFIG } from "./config"

export const FAKE_ARMADA_API_BASE_URL = "https://test-armada-api.armada.com"

window.__LOOKOUT_UI_CONFIG__ = {
  ...DEFAULT_LOOKOUT_UI_CONFIG,
  armadaApiBaseUrl: FAKE_ARMADA_API_BASE_URL,
}
