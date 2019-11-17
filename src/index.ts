import tsv from "tsv";
import _ from "lodash";
import zlib from "zlib";
import stream from "stream";
import request from "request";
import through from "through";
import JSONStream from "JSONStream";
import stream2asynciter from "stream2asynciter";
import ClickHouse from "./ClickHouse";

/**
 * Content-Encoding: gzip
 * Accept-Encoding: gzip
 * и включить настройку ClickHouse enable_http_compression.
 *
 * session_id
 *
 * session_timeout
 */

let SEPARATORS = {
    TSV: "\t",
    CSV: ",",
    Values: ","
};

let ALIASES = {
    TabSeparated: "TSV"
};

let ESCAPE_STRING = {
    TSV: function (v: {
        replace: (arg0: RegExp, arg1: string) => {
            replace: (arg0: RegExp, arg1: string) => {
                replace: (arg0: RegExp, arg1: string) => {
                    replace: (arg0: RegExp, arg1: string) => void;
                };
            };
        };
    }, _quote: any) {
        return v.replace(/\\/g, "\\\\").replace(/\\/g, "\\").replace(/\t/g, "\\t").replace(/\n/g, "\\n")
    },
    CSV: function (v: {
        replace: (arg0: RegExp, arg1: string, arg2: string) => void;
    }, _quote: any) {
        return v.replace(/\"/g, "", "")
    },
};

let ESCAPE_NULL = {
    TSV: "\\N",
    CSV: "\\N",
    Values: "\\N",
    // JSONEachRow: "\\N",
};

const R_ERROR = new RegExp("Code: ([0-9]{2}), .*Exception:");

const URI = "localhost";

const PORT = 8123;

const DATABASE = "default";
const USERNAME = "default";

function parseCSV(body: any, options = {header: true}) {
    const data = new tsv.Parser(SEPARATORS.CSV, options).parse(body);
    data.splice(data.length - 1, 1);
    return data;
}

function parseJSON(body: any) {
    return JSON.parse(body);
}

function parseTSV(body: any, options = {header: true}) {
    const data = new tsv.Parser(SEPARATORS.TSV, options).parse(body);
    data.splice(data.length - 1, 1);
    return data;
}

function parseCSVStream(s: Set<unknown>) {
    let isFirst = true;
    let ref = {
        fields: []
    };
    return through(function (this: any, chunk) {
        let str = chunk.toString();
        let parsed = parseCSV(str, {header: isFirst});
        let strarr = str.split("\n");
        let plen = (isFirst && strarr.length - 1 || strarr.length) - parsed.length;

        if (!isFirst) {
            // @ts-ignore
            chunk = Buffer.concat([Buffer.from([...s].join("\n")), chunk]).toString();
            parsed = parseCSV(str, {header: isFirst});
            s = new Set();
        }
        strarr.splice(strarr.length - plen).forEach(((value: unknown) => s.add(value)));
        chunkBuilder.call(this, isFirst, ref, str, parsed);
        isFirst = false;
    })
}

function parseJSONStream() {
    return JSONStream.parse(["data", true]);
}

function parseTSVStream(s: Set<unknown>) {
    let isFirst = true;
    let ref = {
        fields: []
    };
    return through(function (this: any, chunk) {
        let str = chunk.toString();
        let parsed = parseTSV(str, {header: isFirst});
        let strarr = str.split("\n");
        let plen = (isFirst && strarr.length - 1 || strarr.length) - parsed.length;

        if (!isFirst) {
            chunk = Buffer.concat([Buffer.from([...s].join("\n")), chunk]).toString();
            parsed = parseTSV(str, {header: isFirst});
            s = new Set();
        }
        strarr.splice(strarr.length - plen).forEach((value => s.add(value)));
        chunkBuilder.call(this, isFirst, ref, str, parsed);
        isFirst = false;
    });
}

function chunkBuilder(this: any, isFirst: any, ref: { fields: string[]; }, _chunk: any, parsed: {}[]) {
    if (isFirst) {
        ref.fields = Object.keys(parsed[0]);
        parsed.forEach((value) => {
            this.queue(value);
        });
    } else {
        parsed.forEach((value: any) => {
            let result: any = {};
            ref.fields.forEach((field, index) => (result[field] = value[index]));
            this.queue(result);
            result = null;
        });
    }
}

function encodeValue(quote: boolean, v: Array<any> | Date | number | boolean | null, format: string, isArray: boolean | undefined) {
    format = ALIASES[format] || format;

    switch (typeof v) {
        case "string":
            if (isArray) {
                return `"${ESCAPE_STRING[format] ? ESCAPE_STRING[format](v, quote) : v}"`;
            } else {
                return ESCAPE_STRING[format] ? ESCAPE_STRING[format](v, quote) : v;
            }
        case "number":
            if (isNaN(v))
                return "nan";
            if (v === +Infinity)
                return "+inf";
            if (v === -Infinity)
                return "-inf";
            if (v === Infinity)
                return "inf";
            return v;
        case "object":

            // clickhouse allows to use unix timestamp in seconds
            if (v instanceof Date) {
                return ("" + v.valueOf()).substr(0, 10);
            }

            // you can add array items
            if (v instanceof Array) {
                // return "[" + v.map(encodeValue.bind(this, true, format)).join (",") + "]";
                return "[" + v.map(function (i) {
                    return encodeValue(true, i, format, true);
                }).join(",") + "]";
            }

            // TODO: tuples support
            if (!format) {
                console.trace();
            }

            if (v === null) {
                return format in ESCAPE_NULL ? ESCAPE_NULL[format] : v;
            }

            return format in ESCAPE_NULL ? ESCAPE_NULL[format] : v;
        case "boolean":
            return v ? 1 : 0;
    }
}

function getErrorObj(res: any) {
    let err: any = new Error(`${res.statusCode}: ${res.body || res.statusMessage}`);

    if (res.body) {
        let m = res.body.match(R_ERROR);
        if (m) {
            if (m[1] && !isNaN(parseInt(m[1]))) {
                err.code = parseInt(m[1]);
            }

            if (m[2]) {
                err.message = m[2];
            }
        }
    }

    return err;
}


function isObject(obj: any) {
    return Object.prototype.toString.call(obj) === "[object Object]";
}


class Rs extends stream.Transform {
    private ws: request.Request;
    private isPiped: boolean;
    public query: any;

    constructor(reqParams: string) {
        super();

        let me = this;

        this.ws = request.post(reqParams);

        this.isPiped = false;

        // Без этого обработчика и вызова read Transform не отрабатывает до конца
        // https://nodejs.org/api/stream.html#stream_implementing_a_transform_stream
        // Writing data while the stream is not draining is particularly problematic for a Transform,
        // because the Transform streams are paused by default until they are piped or
        // an "data" or "readable" event handler is added.
        this.on("readable", () => {
            // this._data = me.read();
        });

        this.pipe(me.ws);

        this.on("pipe", () => {
            this.isPiped = true;
        });
    }

    _transform(chunk: any, _encoding: any, cb: (arg0: null, arg1: any) => void) {
        cb(null, chunk);
    }

    writeRow(data: any) {
        let row = '';

        if (Array.isArray(data)) {
            row = ClickHouse.mapRowAsArray(data);
        } else if (isObject(data)) {
            throw new Error("Sorry, but it is not work!");
        }

        let isOk = this.write(
            row + "\n"
        );

        if (isOk) {
            return Promise.resolve();
        } else {
            return new Promise((resolve, reject) => {
                this.ws.once("drain", (err: any) => {
                    if (err) return reject(err);

                    resolve();
                })
            });
        }
    }


    exec() {
        let me = this;

        return new Promise((resolve, reject) => {
            me.ws
                .on("error", (err: any) => {
                    reject(err);
                })
                .on("response", (res: { statusCode: any; body?: { match: (arg0: RegExp) => void; }; statusMessage?: any; }) => {
                    if (res.statusCode === 200) {
                        return resolve({r: 1});
                    }

                    return reject(
                        getErrorObj(res)
                    )
                });

            if (!me.isPiped) {
                me.end();
            }
        });
    }
}


class QueryCursor {
    private isInsert: boolean;
    private query: string;
    private reqParams: {} | any;
    private opts: {} | any;
    private useTotals: boolean;
    private _request: Rs | any;


    constructor(query: any, reqParams: any, opts: any) {
        this.isInsert = !!query.search(/^insert/i);
        this.query = query;
        this.reqParams = _.merge({}, reqParams);
        this.opts = opts;
        this.useTotals = false;
    }


    exec(cb) {
        let me = this;

        if (this.opts.debug) {
            console.log("exec req headers", me.reqParams.headers);
        }

        this._request = request.post(this.reqParams, (err: any, res: { statusCode: any; body: any; statusMessage?: any; headers?: any; }) => {
            if (me.opts.debug) {
                console.log("exec", err, _.pick(res, [
                    "statusCode",
                    "body",
                    "statusMessage"
                ]));
            }

            if (err) {
                return cb(err);
            } else if (res.statusCode !== 200) {
                return cb(
                    getErrorObj(res)
                );
            }

            if (!res.body) {
                return cb(null, {r: 1});
            }

            if (me.opts.debug) {
                console.log("exec res headers", res.headers);
            }

            try {
                const result = this._parseRowsByFormat(res.body);
                cb(null, me.useTotals ? result : result.data || result)
            } catch (e) {
                cb(e);
            }
        });
    }

    _parseRowsByFormat(body: string | null, isStream = false): null | any {
        let result: string | null | any = null;
        switch (this._getFormat()) {
            case "json":
                result = !isStream && parseJSON(body) || parseJSONStream();
                break;
            case "tsv":
                result = !isStream && parseTSV(body) || parseTSVStream(new Set());
                break;
            case "csv":
                result = !isStream && parseCSV(body) || parseCSVStream(new Set());
                break;
            default:
                result = body;
        }
        return result;
    };

    _getFormat() {
        return this.opts.sessionFormat || this.opts.format;
    }

    withTotals() {
        this.useTotals = true;
        return this;
    }

    toPromise() {
        let me = this;

        return new Promise((resolve, reject) => {
            me.exec(function (err: any, data: unknown) {
                if (err) return reject(err);

                resolve(data);
            })
        });
    }


    stream() {
        const
            me = this,
            isDebug = me.opts.debug;

        if (isDebug) {
            console.log("stream req headers", me.reqParams.headers);
        }

        if (me.isInsert) {
            const rs = new Rs(this.reqParams);
            rs.query = this.query;

            me._request = rs;

            return rs;
        } else {
            const streamParser = this._parseRowsByFormat(null, true);

            const rs: stream.Readable | any = new stream.Readable({objectMode: true});
            rs._read = () => {
            };
            rs.query = this.query;

            const tf = new stream.Transform({objectMode: true});
            let isFirstChunck = true;
            tf._transform = function (this: any, chunk, _encoding, cb) {
                // Если для первого chuck первый символ блока данных не "{", тогда:
                // 1. в теле ответа не JSON
                // 2. сервер нашел ошибку в данных запроса
                if (isFirstChunck && (
                    (me._getFormat() === "json" && chunk[0] !== 123) &&
                    (me._getFormat() === "csv" && chunk[0] !== 110) &&
                    (me._getFormat() === "tsv" && chunk[0] !== 110)
                )) {
                    this.error = new Error(chunk.toString());

                    streamParser.emit("error", this.error);
                    rs.emit("close");

                    return cb();
                }

                isFirstChunck = false;

                cb(null, chunk);
            };

            let metaData = {};

            const requestStream = request.post(this.reqParams);

            // Не делаем .pipe(rs) потому что rs - Readable,
            // а для pipe нужен Writable
            let s: any = null;
            if (me.opts.isUseGzip) {
                const z = zlib.createGunzip();
                s = requestStream.pipe(z).pipe(tf).pipe(streamParser)
            } else {
                s = requestStream.pipe(tf).pipe(streamParser)
            }

            if (s != null) {
                s
                    .on("error", (err: any) => {
                        rs.emit("error", err);
                    })
                    .on("header", (header: any) => {
                        metaData = _.merge({}, header);
                    })
                    .on("footer", (footer: any) => {
                        rs.emit("meta", _.merge(metaData, footer));
                    })
                    .on("data", (data: any) => {
                        rs.emit("data", data);
                    })
                    .on("close", () => {
                        rs.emit("close");
                    })
                    .on("end", () => {
                        rs.emit("end");
                    });

                rs.__pause = rs.pause;
                rs.pause = () => {
                    rs.__pause();
                    requestStream.pause();
                    streamParser.pause();
                };

                rs.__resume = rs.resume;
                rs.resume = () => {
                    rs.__resume();
                    requestStream.resume();
                    streamParser.resume();
                };

                me._request = rs;

                return stream2asynciter(rs);
            }
        }
    }


    destroy() {
        if (this._request instanceof stream.Readable) {
            return this._request.destroy();
        }

        if (this._request) {
            return this._request.abort();
        }

        throw new Error("QueryCursor.destroy error: private field _request is invalid");
    }
}


module.exports = {
    ClickHouse
};

