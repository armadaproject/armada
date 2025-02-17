export default class IntervalService {
  ms: number
  callback: () => void

  interval: NodeJS.Timeout | undefined

  constructor(ms: number) {
    this.ms = ms
    this.callback = () => undefined
  }

  registerCallback(callback: () => void) {
    this.callback = callback
  }

  start() {
    this.stop()
    this.interval = setInterval(this.callback, this.ms)
  }

  stop() {
    if (this.interval) {
      clearInterval(this.interval)
    }
  }
}
