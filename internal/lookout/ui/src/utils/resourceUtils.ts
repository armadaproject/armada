import { inverseRecord } from "../utils"

export class ParseError extends Error {
  constructor(msg: string) {
    super(msg)
    Object.setPrototypeOf(this, ParseError.prototype)
  }
}

export class EmptyInputError extends Error {
  constructor(msg: string) {
    super(msg)
    Object.setPrototypeOf(this, EmptyInputError.prototype)
  }
}

const BYTE_MULTIPLIERS: Record<string, number> = {
  K: 1000,
  M: 1000 ** 2,
  G: 1000 ** 3,
  T: 1000 ** 4,
  P: 1000 ** 5,
  E: 1000 ** 6,
  Ki: 1024,
  Mi: 1024 ** 2,
  Gi: 1024 ** 3,
  Ti: 1024 ** 4,
  Pi: 1024 ** 5,
  Ei: 1024 ** 6,
}
const BYTE_UNITS = inverseRecord(BYTE_MULTIPLIERS)

export function parseBytes(str: string): number {
  str = str.trim()
  if (str.length === 0) {
    throw new EmptyInputError("input is empty")
  }
  const floatRegex = /^\d+(\.\d+)?$/
  const fullRegex = /^(\d+(\.\d+)?)\s*([A-Za-z]{1,2})$/
  const regexMatch = str.match(fullRegex)
  if (regexMatch) {
    const value = regexMatch[1]
    const unit = regexMatch[3]
    if (!(unit in BYTE_MULTIPLIERS)) {
      throw new ParseError(`unknown unit: ${unit}`)
    }
    return Math.round(parseFloat(value) * BYTE_MULTIPLIERS[unit])
  }
  if (!str.match(floatRegex)) {
    throw new ParseError(`invalid float value: ${str}`)
  }
  return Math.round(parseFloat(str) * BYTE_MULTIPLIERS.Gi)
}

function formatMult(bytes: number, baseMult: number): string {
  let mult = baseMult
  while (Number.isInteger(bytes / mult)) {
    mult *= baseMult
  }
  mult /= baseMult
  if (!(mult in BYTE_UNITS)) {
    return `${bytes}B`
  }
  return `${bytes / mult}${BYTE_UNITS[mult]}`
}

export function formatBytes(bytes: number): string {
  if (bytes < 1000) {
    return `${bytes}B`
  }
  const decimalStr = formatMult(bytes, 1000)
  const binaryStr = formatMult(bytes, 1024)
  return decimalStr.length >= binaryStr.length ? binaryStr : decimalStr
}

export function parseCpu(str: string): number {
  str = str.trim()
  if (str.length === 0) {
    throw new EmptyInputError("input is empty")
  }
  const milliMatch = str.match(/^(\d+)m$/)
  if (milliMatch) {
    return parseInt(milliMatch[1])
  }
  if (str.match(/^\d+(\.\d+)?$/)) {
    return Math.round(parseFloat(str) * 1000)
  }
  throw new ParseError(`cannot parse ${str}`)
}

export function formatCpu(cpu: number): string {
  if (cpu < 1000) {
    return `${Math.round(cpu)}m`
  }
  return `${Math.round(cpu) / 1000}`
}

export function parseInteger(str: string): number {
  str = str.trim()
  if (str.length === 0) {
    throw new EmptyInputError("input is empty")
  }
  if (str.match(/^\d+$/)) {
    return parseInt(str)
  }
  throw new ParseError("cannot parse str as integer")
}
