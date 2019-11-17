import _ from "lodash";
import querystring from "querystring";

export default class ClickHouse {
    private readonly opts: {
        reqParams: string;
        url: string;
        host: string;
        port: number;
        debug: boolean;
        database: string;
        password: string;
        basicAuth: {
            username: string;
            password: string;
        };
        isUseGzip: boolean;
        config: {
            database: string;
            session_id: number;
            session_timeout: number;
            output_format_json_quote_64bit_integers: number;
            enable_http_compression: number;
        };
        format: "json" | "csv" | "tsv";
        sessionFormat?: "json" | "csv" | "tsv";
    } = {
        host: "http://localhost",
        port: 8123,
        url: "",
        debug: false,
        database: "default",
        password: "",
        basicAuth: {
            username: "",
            password: "",
        },
        isUseGzip: false,
        config: {
            session_id: Date.now(),
            session_timeout: 60,
            output_format_json_quote_64bit_integers: 0,
            enable_http_compression: 0,
        },
        format: "json" // "json" || "csv" || "tsv"
    };


    constructor(opts: any) {
        if (!opts) {
            opts = {};
        }

        this.opts = {
            ...this.opts,
            ...opts,
        };
    }

    get sessionId() {
        return this.opts.config.session_id;
    }

    set sessionId(sessionId) {
        this.opts.config.session_id = sessionId;
    }

    noSession() {
        delete this.opts.config.session_id;

        return this;
    }

    get sessionTimeout() {
        return this.opts.config.session_timeout;
    }

    set sessionTimeout(timeout) {
        this.opts.config.session_timeout = timeout;
    }

    get url() {
        return this.opts.url;
    }

    set url(url) {
        this.opts.url = url;
    }

    get port() {
        return this.opts.port;
    }

    set port(port) {
        this.opts.port = port;
    }

    get isUseGzip() {
        return this.opts.isUseGzip;
    }

    set isUseGzip(val: boolean) {
        this.opts.isUseGzip = val;
        this.opts.config.enable_http_compression = this.opts.isUseGzip && 1 || 0;
    }

    escape(str) {
        return str.replace(/\t|\n/g, "\\t");
    }

    static mapRowAsArray(row: any[]) {
        return row.map(function (value) {
            return encodeValue(false, value, "TabSeparated", false);
        }).join("\t");
    }

    _getFormat(query: string) {
        let format = "";
        switch (this.opts.format) {
            case "json":
                format = this._parseFormat(query, " format JSON");
                break;
            case "tsv":
                format = this._parseFormat(query, " format TabSeparatedWithNames");
                break;
            case "csv":
                format = this._parseFormat(query, " format CSVWithNames");
                break;
            default:
                format = " ";
        }
        return format;
    };

    _parseFormat(query: string, def: string) {
        if (query.match(/format/mg) === null) {
            this.opts.sessionFormat = this.opts.format;
            return def;
        }
        if (query.match(/format JSON/mg) !== null) {
            this.opts.sessionFormat = "json";
        } else if (query.match(/format TabSeparated/mg) !== null) {
            this.opts.sessionFormat = "tsv";
        } else if (query.match(/format CSV/mg) !== null) {
            this.opts.sessionFormat = "csv";
        }
        return "";
    }

    _mapRowAsObject(fieldList: any[], row: { [x: string]: any; }) {
        return fieldList.map(f => encodeValue(false, row[f] != null ? row[f] : "", "TabSeparated", false)).join("\t");
    }


    _getBodyForInsert(query: string, data: any[]) {
        let values: any[] = [],
            fieldList: any[] = [],
            isFirstElObject = false;

        if (Array.isArray(data) && Array.isArray(data[0])) {
            values = data;
        } else if (Array.isArray(data) && typeof data === "object") {
            values = data;
            isFirstElObject = true;
        } else if (typeof data === "object") {
            values = [data];
            isFirstElObject = true;
        } else {
            throw new Error("ClickHouse._getBodyForInsert: data is invalid format");
        }

        if (isFirstElObject) {
            let m = query.match(/INSERT INTO (.+?) \((.+?)\)/);
            if (m) {
                fieldList = m[2].split(",").map(s => s.trim());
            } else {
                throw new Error("insert query wasnt parsed field list after TABLE_NAME");
            }
        }

        return values.map(row => isFirstElObject && this._mapRowAsObject(fieldList, row) || ClickHouse.mapRowAsArray(row))
                     .join("\n");
    }


    _getReqParams(query: string, data: any) {
        let me = this;

        let reqParams = me.opts.reqParams,
            configQS = me.opts.config;

        if (me.opts.database) {
            configQS.database = me.opts.database;
        }

        if (typeof query === "string") {
            let sql = query.trim();

            // Hack for Sequelize ORM
            sql = sql.trimEnd().replace(/;$/gm, "");

            if (sql.match(/^(select|show|exists)/i)) {
                reqParams["url"] = me.url + "?query=" + encodeURIComponent(sql + this._getFormat(sql) + ";") + "&" + querystring.stringify(configQS);
                if (me.opts.username) {
                    reqParams["url"] = reqParams["url"] + "&user=" + me.opts.username;
                }

                if (this.opts.password) {
                    reqParams["url"] = reqParams["url"] + "&password=" + me.opts.password;
                }

                if (data && data.external) {
                    let formData = {};

                    for (let external of data.external) {
                        reqParams.url += `&${external.name}_structure=${external.structure || "str String"}`;

                        formData[external.name] = {
                            value: external.data.join("\n"),
                            options: {
                                filename: external.name,
                                contentType: "text/plain",
                            },
                        };
                    }

                    reqParams["formData"] = formData;
                }
            } else if (query.match(/^insert/i)) {
                reqParams["url"] = me.url + "?query=" + encodeURIComponent(sql + " FORMAT TabSeparated") + "&" + querystring.stringify(configQS);

                if (me.opts.username) {
                    reqParams["url"] = reqParams["url"] + "&user=" + me.opts.username;
                }

                if (this.opts.password) {
                    reqParams["url"] = reqParams["url"] + "&password=" + me.opts.password;
                }

                if (data) {
                    reqParams["body"] = me._getBodyForInsert(sql, data);
                }
            } else {
                reqParams["url"] = me.url + "?query=" + encodeURIComponent(sql + ";") + "&" + querystring.stringify(configQS);

                if (me.opts.username) {
                    reqParams["url"] = reqParams["url"] + "&user=" + me.opts.username;
                }

                if (this.opts.password) {
                    reqParams["url"] = reqParams["url"] + "&password=" + me.opts.password;
                }
            }

            reqParams["headers"] = {
                "Content-Type": "text/plain",
            };
        }

        if (me.opts.isUseGzip) {
            //reqParams.headers["Content-Encoding"] = "gzip";
            reqParams.headers["Accept-Encoding"] = "gzip";
            // reqParams["gzip"] = true;
        }

        if (me.opts.debug) {
            console.log("DEBUG", reqParams);
        }

        return reqParams;
    }


    query(...args: (any)[]) {
        if (args.length === 2 && typeof args[args.length - 1] === "function") {
            return new QueryCursor(args[0], this._getReqParams(args[0], null), this.opts).exec(args[args.length - 1]);
        } else {
            return new QueryCursor(args[0], this._getReqParams(args[0], args[1]), this.opts);
        }
    }


    insert(query: any, data: any[] | null) {
        return new QueryCursor(query, this._getReqParams(query, data), this.opts);
    }
}
