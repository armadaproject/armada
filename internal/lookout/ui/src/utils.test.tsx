import { updateArray } from "./utils";

describe('updateArray', () => {
  test('does nothing if no new values are provided', () => {
    const array = [1, 2, 3, 4, 5]
    updateArray(array, [], 5, 10)
    expect(array).toStrictEqual([1, 2, 3, 4, 5])
  })

  test('does nothing if no new indices are provided', () => {
    const array = [1, 2, 3, 4, 5]
    updateArray(array, [1, 2, 3], 5, 5)
    expect(array).toStrictEqual([1, 2, 3, 4, 5])
  })

  test('concatenates if new values are to be added', () => {
    const array = [1, 2, 3, 4, 5]
    updateArray(array, [1, 2, 3], 5, 8)
    expect(array).toStrictEqual([1, 2, 3, 4, 5, 1, 2, 3])
  })

  test('concatenates new values if more indices are provided', () => {
    const array = [1, 2, 3, 4, 5]
    updateArray(array, [1, 2, 3], 5, 10)
    expect(array).toStrictEqual([1, 2, 3, 4, 5, 1, 2, 3])
  })

  test('replaces existing values if indices overlap', () => {
    const array = [1, 2, 3, 4, 5]
    updateArray(array, [6, 7, 8], 1, 4)
    expect(array).toStrictEqual([1, 6, 7, 8, 5])
  })

  test('adds nulls if indices start beyond initial array end', () => {
    const array = [1, 2, 3, 4, 5]
    updateArray(array, [1, 2, 3], 7, 10)
    expect(array).toStrictEqual([1, 2, 3, 4, 5, null, null, 1, 2, 3])
  })
})
