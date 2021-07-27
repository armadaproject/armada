import { BinocularsApi, Configuration, ConfigurationParameters } from "../openapi/binoculars"

export interface LogLine {
  text: string
  time: string
}

export default class LogService {
  config: ConfigurationParameters
  baseUrlPattern: string
  isEnabled: boolean

  constructor(config: ConfigurationParameters, baseUrlPattern: string, isEnabled: boolean) {
    this.config = config
    this.baseUrlPattern = baseUrlPattern
    this.isEnabled = isEnabled
  }

  async getPodLogs(
    clusterId: string,
    jobId: string,
    namespace: string,
    podNumber: number,
    container: string,
    tailLines: number | undefined = undefined,
    sinceTime: string | undefined = undefined,
  ): Promise<LogLine[]> {
    const maxSize = 2000000 // 2Mb chunks at most
    const api = this.getBinoculars(clusterId)
    const logResult = await api.logs({
      body: {
        jobId: jobId,
        podNumber: podNumber,
        podNamespace: namespace,
        sinceTime: sinceTime,
        logOptions: {
          container: container,
          tailLines: tailLines,
          timestamps: true,
          limitBytes: maxSize,
        },
      },
    })
    return this.parseLogLines(logResult.log ?? "", maxSize)
  }

  private parseLogLines(log: string, maxSize: number) {
    const lines = log.split("\n").filter((s) => s != "")
    if (log.length >= maxSize) {
      // discart last partial line
      lines.pop()
    }
    return lines.map((l) => {
      const divider = l.indexOf(" ")
      return {
        time: l.substr(0, divider),
        text: l.substr(divider + 1),
      }
    })
  }

  private getBinoculars(clusterId: string) {
    return new BinocularsApi(
      new Configuration({
        ...this.config,
        basePath: this.baseUrlPattern.replace("{CLUSTER_ID}", clusterId),
      }),
    )
  }
}
