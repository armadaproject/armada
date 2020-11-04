import { LookoutApi } from '../openapi'

export class JobService{

    api: LookoutApi;

    constructor(lookoutAPi: LookoutApi) {
        this.api = lookoutAPi
    }

    getOverview() {
        return this.api.overview()
    }

}
