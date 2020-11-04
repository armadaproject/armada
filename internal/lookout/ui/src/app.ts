import { Main } from './main';
import ReactDOM from 'react-dom';
import React from 'react';
import { Configuration, LookoutApi } from '../openapi';
import { JobService } from './services/jobs';

export class App
{
    jobs = new JobService(new LookoutApi(new Configuration({basePath: "/"})))

    constructor()
    {
        this.render();
    }

    private render(): void
    {
        ReactDOM.render(React.createElement(Main, { app: this }),
            document.getElementById("app") || document.createElement("div")
        );
    }
}

new App();
