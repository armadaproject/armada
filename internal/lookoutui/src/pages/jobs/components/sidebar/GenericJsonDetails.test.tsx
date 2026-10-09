import { render, within } from "@testing-library/react"
import userEvent from "@testing-library/user-event"

import { GenericJsonDetails } from "./GenericJsonDetails"

describe("GenericJsonDetails", () => {
  it("renders all array elements when the array is expanded", async () => {
    const { getAllByRole, getByRole, findByRole, queryByRole } = render(
      <GenericJsonDetails value={{ attempts: [{ host: "worker-1" }, { host: "worker-2" }] }} />,
    )

    await userEvent.click(getByRole("button", { name: "attempts" }))

    within(await findByRole("row", { name: "host worker-1" })).getByText("worker-1")
    within(await findByRole("row", { name: "host worker-2" })).getByText("worker-2")
    expect(queryByRole("button", { name: "1" })).toBeNull()
    expect(getAllByRole("separator")).toHaveLength(1)
  })
})
