import {Response} from "request";

export default class RError extends Error {

    private _code: RegExp = new RegExp("Code: ([0-9]{2}), .*Exception:");
    private _message: string | undefined;

    private _statusCode: number;
    private _statusMessage: string;
    private _timingStart: number | undefined;
    private _elapsedTime: number | undefined;

    private _response: Response;

    constructor(response: Response) {
        super();
        this._response = response;
        this._statusCode = response.statusCode;
        this._statusMessage = response.statusMessage;
        this._timingStart = response.timingStart;
        this._elapsedTime = response.elapsedTime;
    }

    get code(): RegExp {
        return this._code;
    }

    set code(_value: RegExp) {
        // this._code = _value;
    }

    get msg(): string | undefined {
        return this._message;
    }

    set msg(value: string | undefined) {
        this._message = value;
    }

    get elapsedTime(): number | undefined {
        return this._elapsedTime;
    }

    set elapsedTime(value: number | undefined) {
        this._elapsedTime = value;
    }

    get timingStart(): number | undefined {
        return this._timingStart;
    }

    set timingStart(value: number | undefined) {
        this._timingStart = value;
    }

    get statusMessage(): string {
        return this._statusMessage;
    }

    set statusMessage(value: string) {
        this._statusMessage = value;
    }

    get statusCode(): number {
        return this._statusCode;
    }

    set statusCode(value: number) {
        this._statusCode = value;
    }

    get response(): Response {
        return this._response;
    }

    set response(value: Response) {
        this._response = value;
    }
}
