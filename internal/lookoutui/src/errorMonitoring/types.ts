import { RootOptions } from "react-dom/client"

export type ReactErrorHandlers = Pick<RootOptions, "onCaughtError" | "onRecoverableError" | "onUncaughtError">
