import DetailRow from "./DetailRow"
describe("DetailRow", () => {
  it("DetailRow with no links", () => {
      const actual = DetailRow({key: 'test', name: 'test', value: 'test'})
      console.log(actual.props.children)
  })
  it("DetailRow with links", () => {
    const actual = DetailRow({name: 'test', value: 'http://google.org'})
    console.log(actual.props.children)
})

})
