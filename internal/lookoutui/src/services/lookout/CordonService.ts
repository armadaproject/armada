import { appendAuthorizationHeaders } from "../../oidcAuth"
import { ConfigurationParameters } from "../../openapi/binoculars"
import { getBinocularsApi } from "../../utils"

export interface ICordonService {
  cordonNode(cluster: string, node: string, accessToken?: string, signal?: AbortSignal): Promise<void>
}

export class CordonService implements ICordonService {
  config: ConfigurationParameters
  baseUrlPattern: string

  constructor(config: ConfigurationParameters, baseUrlPattern: string) {
    this.config = config
    this.baseUrlPattern = baseUrlPattern
  }

  async cordonNode(cluster: string, node: string, accessToken?: string): Promise<void> {
    const headers = new Headers()
    if (accessToken) {
      appendAuthorizationHeaders(headers, accessToken)
    }

    const api = getBinocularsApi(cluster, this.baseUrlPattern, this.config)
    await api.cordon(
      {
        body: {
          nodeName: node,
        },
      },
      { headers },
    )
  }
}
