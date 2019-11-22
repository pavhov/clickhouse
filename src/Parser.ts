
import tsv from "tsv";
import {types} from "util";
import JSONStream from "JSONStream";
import through, {ThroughStream} from "through";


export default class Parser {
    private static _separators: { [key: string]: string; } = {
        TSV: "\t",
        CSV: ",",
    };
    private static _aliases: { [key: string]: string; } = {
        TSV: "TSV",
        CSV: "CSV",
    };
    private static _escape: { [key: string]: { [key: string]: (v: string) => string; } } = {
        string: {
            TSV: function (v) {
                return v
                    .replace(/\\/g, '\\\\')
                    .replace(/\\/g, '\\')
                    .replace(/\t/g, '\\t')
                    .replace(/\n/g, '\\n')
            },
            CSV: function (v) {
                return v.replace(/\"/g, '""')
            },
        },
        null: {}
    };
    private static _instance: Parser | null = null;


    private constructor() {
    }

    public static instance() {
        if (this._instance === null) {
            this._instance = new Parser();
        }
        return this._instance;
    }


    public parseCSV(body, options = {header: true}) {
        const data = new tsv.Parser(Parser._separators.CSV, options).parse(body);
        data.splice(data.length - 1, 1);
        return data;
    }

    public parseJSON(body) {
        return JSON.parse(body);
    }

    public parseTSV(body, options = {header: true}) {
        const data = new tsv.Parser(Parser._separators.TSV, options).parse(body);
        data.splice(data.length - 1, 1);
        return data;
    }

    public parseCSVStream(s) {
        let isFirst = true;
        let ref = {
            fields: [],
        };
        return through(function (this: ThroughStream, chunk) {
            let str = chunk.toString();
            let parsed = Parser.instance().parseCSV(str, {header: isFirst});
            let strarr = str.split("\n");
            let plen = (isFirst && strarr.length - 1 || strarr.length) - parsed.length;

            if (!isFirst) {
                chunk = Buffer.concat([Buffer.from([...s].join("\n")), chunk]).toString();
                parsed = Parser.instance().parseCSV(str, {header: isFirst});
                s = new Set();
            }
            strarr.splice(strarr.length - plen).forEach((value => s.add(value)));
            Parser.instance().chunkBuilder.call(this, isFirst, ref, str, parsed);
            isFirst = false;
        });
    }

    public parseJSONStream() {
        return JSONStream.parse(["data", true]);
    }

    public parseTSVStream(s) {
        let isFirst = true;
        let ref = {
            fields: [],
        };
        return through(function (this: ThroughStream, chunk) {
            let str = chunk.toString();
            let parsed = Parser.instance().parseTSV(str, {header: isFirst});
            let strarr = str.split("\n");
            let plen = (isFirst && strarr.length - 1 || strarr.length) - parsed.length;

            if (!isFirst) {
                chunk = Buffer.concat([Buffer.from([...s].join("\n")), chunk]).toString();
                parsed = Parser.instance().parseTSV(str, {header: isFirst});
                s = new Set();
            }
            strarr.splice(strarr.length - plen).forEach((value => s.add(value)));
            Parser.instance().chunkBuilder.call(this, isFirst, ref, str, parsed);
            isFirst = false;
        });
    }

    private chunkBuilder(this: ThroughStream | any, isFirst, ref, _chunk, parsed) {
        if (isFirst) {
            ref.fields = Object.keys(parsed[0]);
            parsed.forEach((value) => {
                this.queue(value);
            });
        } else {
            parsed.forEach((value) => {
                let result: any | null = {};
                ref.fields.forEach((field, index) => (result[field] = value[index]));
                this.queue(result);
                result = null;
            });
        }
    }

    public encodeValue(v: any, format: string, isArr: boolean = false) {
        let result: string | number = "";
        format = Parser._aliases[format] || format;

        switch (typeof v) {
            case "string":
                result = `${Parser._escape.string[format](v)}`;
                if (isArr) {
                    result = `"${isArr}"`
                }
                break;
            case "number":
                if (isNaN(v)) {
                    result = "nan";
                } else if (v === +Infinity) {
                    result = "+inf";
                } else if (v === -Infinity) {
                    result = "-inf";
                } else if (v === Infinity) {
                    result = "inf";
                } else {
                    result = v
                }
                break;
            case "object":
                if (Array.isArray(v)) {
                    result = "[" + v.map((i) => this.encodeValue(i, format, true)).join(",") + "]";
                } else if (types.isDate(v)) {
                    result = ("" + v.getTime()).substr(0, 10);
                } else if (types.isMap(v)) {
                    result = "[" + [...v.entries()].map((i) => this.encodeValue(i, format, true)).join(",") + "]";
                } else if (types.isSet(v)) {
                    result = "[" + [...v].map((i) => this.encodeValue(i, format, true)).join(",") + "]";
                } else {
                    result = JSON.stringify(v);
                }
                break;
            case "boolean":
                result = v && 1 || 0;
                break;
            default:
                //todo: will implement
                break;
        }
        return result;
    }

    mapRowAsArray(row, format) {
        return row.map((value) => Parser.instance().encodeValue(value, format)).join("\t");
    }

    _mapRowAsObject(fieldList, row, format) {
        return fieldList.map(f => Parser.instance().encodeValue(row[f], format)).join("\t");
    }

}
