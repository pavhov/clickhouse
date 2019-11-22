import util from "util";

declare module "util" {
    namespace types {
        function isObject(value: any): boolean;
    }
}

util.types["isObject"] = (v) => typeof v === "object";
