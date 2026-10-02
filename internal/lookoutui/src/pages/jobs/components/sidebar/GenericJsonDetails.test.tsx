import { render, within } from "@testing-library/react"
import userEvent from "@testing-library/user-event"

import { GenericJsonDetails } from "./GenericJsonDetails"

describe("GenericJsonDetails", () => {
  it("renders nested array entries with one-indexed labels", async () => {
    const { getByRole, findByRole } = render(<GenericJsonDetails value={{ attempts: [{ host: "worker-1" }] }} />)

    await userEvent.click(getByRole("button", { name: "attempts" }))
    await userEvent.click(getByRole("button", { name: "1" }))

    within(await findByRole("row", { name: "host worker-1" })).getByText("worker-1")
  })
})
