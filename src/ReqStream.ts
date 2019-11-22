import {types} from "util";
import stream from "stream";
import request, {Response} from "request";

import Parser from "./Parser";
import RError from "./Error";

export default class ReqStream extends stream.Transform {
    // @ts-ignore
    private _ws: request.Request;
    // @ts-ignore
    private _isPiped: boolean;
    // @ts-ignore
    private _rowCount: number;

    private _format: string;

    // @ts-ignore
    _query: string;

    constructor(reqParams: any, format: string) {
        super();

        this._ws = request.post(reqParams);

        this._isPiped = false;
        this._format = format;

        // Без этого обработчика и вызова read Transform не отрабатывает до конца
        // https://nodejs.org/api/stream.html#stream_implementing_a_transform_stream
        // Writing data while the stream is not draining is particularly problematic for a Transform,
        // because the Transform streams are paused by default until they are piped or
        // an "data" or "readable" event handler is added.
        this.on("readable", () => {
            // let _data = me.read();
        });

        this.pipe(this._ws);

        this.on("pipe", () => {
            this._isPiped = true;
        });
    }

    _transform(chunk, _encoding, cb) {
        cb(null, chunk);
    }

    writeRow(data) {
        let row = "";

        if (Array.isArray(data)) {
            row = Parser.instance().mapRowAsArray(data, this._format);
        } else if (types.isObject(data)) {
            throw new Error("Sorry, but it is not work!");
        }

        let isOk = this.write(
            row + "\n",
        );

        this._rowCount++;

        if (isOk) {
            return Promise.resolve();
        } else {
            return new Promise((resolve, reject) => {
                this._ws.once("drain", err => {
                    if (err) return reject(err);

                    resolve();
                });
            });
        }
    }


    exec() {
        let me = this;

        return new Promise((resolve, reject) => {
            me._ws
                .on("error", (err) => {
                    reject(err);
                })
                .on("response", (res: Response) => {
                    if (res.statusCode === 200) {
                        return resolve({r: 1});
                    }

                    return reject(new RError(res));
                });

            if (!me._isPiped) {
                me.end();
            }
        });
    }
}

