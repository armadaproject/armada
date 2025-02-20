export default class TimerService {
  ms: number
  callback: () => void

  timeout: NodeJS.Timeout | undefined

  constructor(ms: number) {
    this.ms = ms
    this.callback = () => undefined
  }

  registerCallback(callback: () => void) {
    this.callback = callback
  }

  start() {
    this.stop()
    this.timeout = setTimeout(this.callback, this.ms)
  }

  stop() {
    if (this.timeout) {
      clearTimeout(this.timeout)
    }
  }
}
