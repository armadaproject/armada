import { EmptyInputError, ParseError, parseBytes, formatBytes, parseCpu, formatCpu } from "./resourceUtils"

const MULTIPLIERS = [
  ["K", 1000],
  ["M", 1000 ** 2],
  ["G", 1000 ** 3],
  ["T", 1000 ** 4],
  ["P", 1000 ** 5],
  ["E", 1000 ** 6],
  ["Ki", 1024],
  ["Mi", 1024 ** 2],
  ["Gi", 1024 ** 3],
  ["Ti", 1024 ** 4],
  ["Pi", 1024 ** 5],
  ["Ei", 1024 ** 6],
]

describe("parseBytes", () => {
  it("should raise an error if empty", () => {
    expect(() => parseBytes("")).toThrow(EmptyInputError)
  })

  it("should return integer value in gibibytes", () => {
    expect(parseBytes("123")).toEqual(123 * 1024 ** 3)
  })

  it("should return floating point value in gibibytes", () => {
    expect(parseBytes("123.6")).toEqual(Math.round(123.6 * 1024 ** 3))
  })

  it.each(MULTIPLIERS)("should return correct value for %s", (unit, multiplier) => {
    expect(parseBytes(`123${unit}`)).toEqual(123 * (multiplier as number))
  })

  it.each(MULTIPLIERS)("should return correct value for %s with space between value and unit", (unit, multiplier) => {
    expect(parseBytes(`123 ${unit}`)).toEqual(123 * (multiplier as number))
  })

  it.each(MULTIPLIERS)("should return correct value for %s with several spaces", (unit, multiplier) => {
    expect(parseBytes(`  123   ${unit}  `)).toEqual(123 * (multiplier as number))
  })

  it.each(MULTIPLIERS)("should return correct value for %s with float value", (unit, multiplier) => {
    expect(parseBytes(`123.456 ${unit}`)).toEqual(Math.round(123.456 * (multiplier as number)))
  })

  it.each(["123Oi", "hello world", "123.31.64", "-123.4"])(
    "should throw error for invalid value %s",
    (invalidValue) => {
      expect(() => parseBytes(invalidValue)).toThrow(ParseError)
    },
  )
})

describe("formatBytes", () => {
  it("should return bytes if less than one K", () => {
    expect(formatBytes(123)).toEqual("123B")
  })

  it.each(MULTIPLIERS)("should return correct string for %s", (unit, multiplier) => {
    expect(formatBytes(123 * (multiplier as number))).toEqual(`123${unit}`)
  })

  it.each([
    [60000 * 1024 ** 2, "60000Mi"],
    [64 * 1024 ** 3, "64Gi"],
    [3000 * 1000 ** 3, "3T"],
    [43264 * 1024 ** 2, "43264Mi"],
    [2400 * 1024 ** 3, "2400Gi"],
  ])("should show easiest string to read for %s", (bytes, expected) => {
    expect(formatBytes(bytes)).toEqual(expected)
  })
})

describe("parseCpu", () => {
  it("should raise an error if empty", () => {
    expect(() => parseCpu("")).toThrow(EmptyInputError)
  })

  it.each([
    ["1", 1000],
    [" 2", 2000],
    [" 1500m", 1500],
    [" 27 ", 27000],
    ["50m ", 50],
    ["1.1", 1100],
    ["27.345", 27345],
    ["27.9999999999", 28000],
  ])("should return correct number of millicores for %s", (s, expected) => {
    expect(parseCpu(s)).toEqual(expected)
  })

  it.each([["hello", "123.456.789", "123.", "123.5m"]])("should throw for invalid input %s", (s) => {
    expect(() => parseCpu(s)).toThrow(ParseError)
  })
})

describe("formatCpu", () => {
  it.each([
    [10, "10m"],
    [200, "200m"],
    [1000, "1"],
    [27000, "27"],
    [32100, "32.1"],
    [100.99, "101m"],
    [12345.6, "12.346"],
  ])("should format correctly %s", (cpu, expected) => {
    expect(formatCpu(cpu)).toEqual(expected)
  })
})
