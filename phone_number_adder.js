#!/usr/bin/env node
"use strict";

/**
 * Enrich Members from CSV via v1 API
 *
 * Reads an input CSV, extracts a member ID column, calls:
 *   GET /api/v1/membership/getmember?searchParam=MemberId&searchValue={id}
 * with headers Scheme and x-api-key, and writes a new CSV with selected fields.
 *
 * No external dependencies.
 */

const https = require("https");
const fs = require("fs");
const path = require("path");
const { URL } = require("url");

// Defaults
const DEFAULT_BASE_URL = "https://budgethealth.webportal.co.zw/api/v1";
const DEFAULT_ENDPOINT = "membership/getmember";
const DEFAULT_SEARCH_PARAM = "MemberId";

const DEFAULT_TIMEOUT = 5000;
const DEFAULT_RETRIES = 1;
const DEFAULT_CONCURRENCY = 10;

const DEFAULT_FIELDS = [
    "memberId",
    "segregatedFund",
    "title",
    "memberNo",
    "suffix",
    "firstname",
    "initials",
    "surname",
    "sex",
    "nationality",
    "occupation",
    "memberStatus",
    "dateOfBirth",
    "dateOfJoining",
    "dateOfResigning",
    "nationalIdNo",
    "plan",
    "isDependant",
    "cellphoneNo",
    "emailAddress",
    "emailAddress2",
    "company",
    "notes",
    "parentId",
];

// ---------------- CLI parsing ----------------
function parseArgs(argv) {
    const opts = {
        baseUrl: DEFAULT_BASE_URL,
        endpoint: DEFAULT_ENDPOINT,
        scheme: process.env.BH_SCHEME || undefined,
        apiKey: process.env.BH_API_KEY || undefined,
        searchParam: DEFAULT_SEARCH_PARAM,

        in: undefined, // input CSV file path
        out: undefined, // output CSV file path (default: stdout)
        append: false,
        idColumn: "memberId",
        fields: undefined, // custom output fields
        keepFailed: false,
        dedupe: true, // set --dedupe=false to disable

        timeout: DEFAULT_TIMEOUT,
        retries: DEFAULT_RETRIES,
        concurrency: DEFAULT_CONCURRENCY,

        silent: false,
        help: false,
    };

    const args = argv.slice(2);
    for (let i = 0; i < args.length; i++) {
        const a = args[i];
        const next = () => args[i + 1];

        if (a === "-h" || a === "--help") opts.help = true;
        else if (a === "--scheme") {
            opts.scheme = next();
            i++;
        } else if (a.startsWith("--scheme=")) opts.scheme = a.split("=")[1];
        else if (a === "--api-key") {
            opts.apiKey = next();
            i++;
        } else if (a.startsWith("--api-key=")) opts.apiKey = a.split("=")[1];
        else if (a === "--base-url" || a === "--url") {
            opts.baseUrl = next();
            i++;
        } else if (a.startsWith("--base-url=") || a.startsWith("--url="))
            opts.baseUrl = a.split("=")[1];
        else if (a === "--endpoint") {
            opts.endpoint = next();
            i++;
        } else if (a.startsWith("--endpoint=")) opts.endpoint = a.split("=")[1];
        else if (a === "--searchParam") {
            opts.searchParam = next();
            i++;
        } else if (a.startsWith("--searchParam="))
            opts.searchParam = a.split("=")[1];
        else if (a === "--in" || a === "--input") {
            opts.in = next();
            i++;
        } else if (a.startsWith("--in=") || a.startsWith("--input="))
            opts.in = a.split("=")[1];
        else if (a === "--out") {
            opts.out = next();
            i++;
        } else if (a.startsWith("--out=")) opts.out = a.split("=")[1];
        else if (a === "--append") opts.append = true;
        else if (a === "--id-column") {
            opts.idColumn = next();
            i++;
        } else if (a.startsWith("--id-column=")) opts.idColumn = a.split("=")[1];
        else if (a === "--fields") {
            opts.fields = next();
            i++;
        } else if (a.startsWith("--fields=")) opts.fields = a.split("=")[1];
        else if (a === "--keep-failed") opts.keepFailed = true;
        else if (a.startsWith("--dedupe=")) {
            const v = a.split("=")[1];
            opts.dedupe = !(v === "false" || v === "0" || v.toLowerCase() === "no");
        } else if (a === "--timeout") {
            opts.timeout = parseInt(next(), 10);
            i++;
        } else if (a.startsWith("--timeout="))
            opts.timeout = parseInt(a.split("=")[1], 10);
        else if (a === "--retries") {
            opts.retries = parseInt(next(), 10);
            i++;
        } else if (a.startsWith("--retries="))
            opts.retries = parseInt(a.split("=")[1], 10);
        else if (a === "--concurrency") {
            opts.concurrency = parseInt(next(), 10);
            i++;
        } else if (a.startsWith("--concurrency="))
            opts.concurrency = parseInt(a.split("=")[1], 10);
        else if (a === "--silent") opts.silent = true;
        else {
            console.error(`Unknown option: ${a}`);
            opts.help = true;
        }
    }

    // Normalize numeric options
    if (!Number.isFinite(opts.timeout) || opts.timeout < 500)
        opts.timeout = DEFAULT_TIMEOUT;
    if (!Number.isFinite(opts.retries) || opts.retries < 0)
        opts.retries = DEFAULT_RETRIES;
    if (!Number.isFinite(opts.concurrency) || opts.concurrency < 1)
        opts.concurrency = DEFAULT_CONCURRENCY;

    return opts;
}

function printHelp() {
    const msg = `
Enrich Members from CSV via v1 API

Usage:
  node enrich_members_from_csv.js --scheme MMSBudgetHealth --api-key <KEY> --in members.csv --out enriched.csv [options]

Required:
  --scheme <value>               Header "Scheme" value (or env BH_SCHEME)
  --api-key <value>              Header "x-api-key" value (or env BH_API_KEY)
  --in <file>                    Input CSV (use "-" to read from stdin)

API options:
  --base-url|--url <url>         Base URL (default: ${DEFAULT_BASE_URL})
  --endpoint <path>              Endpoint path (default: ${DEFAULT_ENDPOINT})
  --searchParam <name>           Query param for lookup (default: ${DEFAULT_SEARCH_PARAM})

Input options:
  --id-column <name>             Column name containing member IDs (default: memberId)
  --dedupe=<true|false>          Dedupe IDs while preserving first order (default: true)

Output options:
  --out <file>                   Output CSV (default: stdout)
  --append                       Append to output (skip header if file has content)
  --fields <a,b,c>               Output columns (default set used if omitted)
  --keep-failed                  Include rows for failed lookups (memberId only; other fields blank)

Network and runtime:
  --concurrency <n>              Parallel requests (default: ${DEFAULT_CONCURRENCY})
  --timeout <ms>                 Request timeout (default: ${DEFAULT_TIMEOUT})
  --retries <n>                  Retry attempts for 429/5xx/timeouts (default: ${DEFAULT_RETRIES})
  --silent                       Reduce logs
  -h, --help                     Show this help

Example:
  node enrich_members_from_csv.js --scheme MMSBudgetHealth --api-key YOUR_KEY \\
    --in members.csv --out enriched.csv --concurrency 5 --fields memberId,memberNo,firstname,surname,plan
`;
    console.log(msg.trim());
}

function trimSlashes(s, leading = true, trailing = true) {
    let r = s;
    if (leading) r = r.replace(/^\/+/, "");
    if (trailing) r = r.replace(/\/+$/, "");
    return r;
}

// ---------------- CSV utils ----------------

// Parses CSV text into an array of objects using the first row as headers.
// Handles quoted fields, escaped quotes, commas, and newlines.
function parseCSVToObjects(text, delimiter = ",") {
    const rows = [];
    let row = [];
    let field = "";
    let inQuotes = false;

    for (let i = 0; i < text.length; i++) {
        const c = text[i];

        if (inQuotes) {
            if (c === '"') {
                // Check for escaped quote
                const next = text[i + 1];
                if (next === '"') {
                    field += '"';
                    i++;
                } else {
                    inQuotes = false;
                }
            } else {
                field += c;
            }
        } else {
            if (c === '"') {
                inQuotes = true;
            } else if (c === delimiter) {
                row.push(field);
                field = "";
            } else if (c === "\r") {
                // Handle CRLF or lone CR
                row.push(field);
                field = "";
                rows.push(row);
                row = [];
                if (text[i + 1] === "\n") i++; // skip LF in CRLF
            } else if (c === "\n") {
                row.push(field);
                field = "";
                rows.push(row);
                row = [];
            } else {
                field += c;
            }
        }
    }

    // Flush last field/row if any content remains
    if (field.length > 0 || row.length > 0) {
        row.push(field);
        rows.push(row);
    }

    if (rows.length === 0) return [];

    // Extract headers
    const headers = rows[0].map((h) => {
        let x = h;
        // Remove BOM if present on first header
        if (x && x.charCodeAt(0) === 0xfeff) x = x.slice(1);
        return x;
    });

    const objs = [];
    for (let r = 1; r < rows.length; r++) {
        const cols = rows[r];
        // skip empty rows
        if (cols.length === 1 && cols[0].trim() === "") continue;
        const obj = {};
        for (let c = 0; c < headers.length; c++) {
            obj[headers[c]] = cols[c] !== undefined ? cols[c] : "";
        }
        objs.push(obj);
    }
    return objs;
}

function csvEscape(value) {
    if (value === null || value === undefined) return "";
    const s = String(value);
    const needsQuotes = /[",\n\r]/.test(s);
    const escaped = s.replace(/"/g, '""');
    return needsQuotes ? `"${escaped}"` : escaped;
}

function objToCsvRow(obj, fields) {
    // Writes empty string if property missing
    return fields
        .map((f) =>
            csvEscape(
                Object.prototype.hasOwnProperty.call(obj || {}, f) ? obj[f] : ""
            )
        )
        .join(",");
}

function writeHeaderIfNeeded(stream, fields, append, outPath) {
    let shouldWriteHeader = true;
    if (append && outPath && fs.existsSync(outPath)) {
        try {
            const stat = fs.statSync(outPath);
            if (stat.size > 0) shouldWriteHeader = false;
        } catch (_) { }
    }
    if (shouldWriteHeader) {
        stream.write(fields.join(",") + "\n");
    }
}

// ---------------- HTTP utils ----------------
function makeRequest({
    baseUrl,
    endpoint,
    searchParam,
    searchValue,
    scheme,
    apiKey,
    timeout,
}) {
    return new Promise((resolve) => {
        let base = trimSlashes(baseUrl, false, true);
        let ep = trimSlashes(endpoint, true, true);
        const full = `${base}/${ep}?searchParam=${encodeURIComponent(
            searchParam
        )}&searchValue=${encodeURIComponent(searchValue)}`;
        const url = new URL(full);

        const options = {
            method: "GET",
            headers: {
                Accept: "application/json",
                Scheme: scheme,
                "x-api-key": apiKey,
            },
        };

        const req = https.request(url, options, (res) => {
            let data = "";
            res.setEncoding("utf8");
            res.on("data", (chunk) => (data += chunk));
            res.on("end", () => {
                const status = res.statusCode || 0;
                let parsed = null;
                let parseErr = null;
                if (data && data.trim()) {
                    try {
                        parsed = JSON.parse(data);
                    } catch (e) {
                        parseErr = e;
                    }
                }
                resolve({
                    ok: status >= 200 && status < 300 && !parseErr,
                    status,
                    data: parsed,
                    error: parseErr
                        ? new Error(`JSON parse error: ${parseErr.message}`)
                        : null,
                    bodyText: data,
                    headers: res.headers,
                });
            });
        });

        req.setTimeout(timeout, () => {
            req.destroy(new Error("Request timeout"));
        });

        req.on("error", (err) => {
            resolve({
                ok: false,
                status: 0,
                data: null,
                error: err,
                bodyText: "",
                headers: {},
            });
        });

        req.end();
    });
}

function isRetryable(status, err) {
    if (err) return true;
    if (status === 429) return true;
    if (status >= 500 && status <= 599) return true;
    return false;
}

async function fetchWithRetry(params, retries, silent) {
    let attempt = 0;
    let last;
    while (attempt <= retries) {
        last = await makeRequest(params);
        if (last.ok) return last;
        const retryable = isRetryable(last.status, last.error);
        if (!retryable || attempt === retries) break;
        const backoff = Math.min(500 * Math.pow(2, attempt), 8000);
        if (!silent)
            console.error(
                `Retrying id=${params.searchValue} after ${last.error ? last.error.message : "HTTP " + last.status
                } in ${backoff}ms`
            );
        await new Promise((r) => setTimeout(r, backoff));
        attempt++;
    }
    return last;
}

// ---------------- Main ----------------
async function main() {
    const opts = parseArgs(process.argv);

    if (opts.help) {
        printHelp();
        process.exit(0);
    }

    if (!opts.in) {
        console.error("Missing --in <file>. Use '-' to read from stdin.");
        process.exit(1);
    }
    if (!opts.scheme || !opts.apiKey) {
        console.error(
            "Missing required headers. Provide --scheme and --api-key (or set BH_SCHEME/BH_API_KEY)."
        );
        process.exit(1);
    }

    // Read input CSV
    let inputText = "";
    if (opts.in === "-" || opts.in === "/dev/stdin") {
        // Read from stdin
        inputText = await new Promise((resolve, reject) => {
            let data = "";
            process.stdin.setEncoding("utf8");
            process.stdin.on("data", (chunk) => (data += chunk));
            process.stdin.on("end", () => resolve(data));
            process.stdin.on("error", reject);
        });
    } else {
        if (!fs.existsSync(opts.in)) {
            console.error(`Input file not found: ${opts.in}`);
            process.exit(1);
        }
        inputText = fs.readFileSync(opts.in, "utf8");
    }

    // Parse CSV to objects
    const inputRows = parseCSVToObjects(inputText);
    if (inputRows.length === 0) {
        console.error("No data rows found in input CSV.");
        process.exit(1);
    }

    // Extract member IDs
    const idCol = opts.idColumn;
    const rawIds = inputRows
        .map((row, idx) => ({ id: (row[idCol] || "").trim(), idx }))
        .filter((x) => x.id !== "");

    if (rawIds.length === 0) {
        console.error(`No member IDs found in column "${idCol}".`);
        process.exit(1);
    }

    // Dedupe while preserving first-seen order
    let tasks;
    if (opts.dedupe) {
        const seen = new Set();
        tasks = [];
        for (const item of rawIds) {
            if (!seen.has(item.id)) {
                seen.add(item.id);
                tasks.push({ id: item.id, firstIdx: item.idx });
            }
        }
    } else {
        tasks = rawIds.map((x) => ({ id: x.id, firstIdx: x.idx }));
    }

    if (!opts.silent) {
        const baseInfo = `${trimSlashes(opts.baseUrl, false, true)}/${trimSlashes(
            opts.endpoint,
            true,
            true
        )}`;
        console.error(
            `Input rows: ${inputRows.length}. Candidate IDs: ${rawIds.length}. After dedupe: ${tasks.length}.`
        );
        console.error(
            `Fetching via: ${baseInfo}?searchParam=${opts.searchParam}&searchValue={id}`
        );
        console.error(
            `Concurrency: ${opts.concurrency}, Retries: ${opts.retries}, Timeout: ${opts.timeout}ms`
        );
    }

    // Prepare HTTP config
    const httpBase = {
        baseUrl: opts.baseUrl,
        endpoint: opts.endpoint,
        searchParam: opts.searchParam,
        scheme: opts.scheme,
        apiKey: opts.apiKey,
        timeout: opts.timeout,
    };

    // Perform concurrent fetches (preserve output order by index in "tasks" array)
    const results = new Array(tasks.length); // will hold API data or null on failure
    let nextIndex = 0;

    async function worker(workerId) {
        while (true) {
            const i = nextIndex++;
            if (i >= tasks.length) break;
            const { id } = tasks[i];
            const res = await fetchWithRetry(
                { ...httpBase, searchValue: id },
                opts.retries,
                opts.silent
            );
            if (res.ok) {
                results[i] = res.data;
                if (!opts.silent) console.error(`OK (${res.status}) id=${id}`);
            } else {
                results[i] = null;
                const snippet = (res.bodyText || "").slice(0, 200).replace(/\s+/g, " ");
                const msg = res.error
                    ? res.error.message
                    : `HTTP ${res.status}${snippet ? " - " + snippet : ""}`;
                console.error(`Failed id=${id}: ${msg}`);
            }
        }
    }

    const workers = Math.min(opts.concurrency, tasks.length);
    await Promise.all(Array.from({ length: workers }, (_, k) => worker(k)));

    // Prepare output stream
    const fields = opts.fields
        ? opts.fields
            .split(",")
            .map((f) => f.trim())
            .filter(Boolean)
        : DEFAULT_FIELDS;

    let stream;
    let outPath = null;
    if (opts.out) {
        outPath = path.resolve(process.cwd(), opts.out);
        const dir = path.dirname(outPath);
        if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
        stream = fs.createWriteStream(outPath, {
            flags: opts.append ? "a" : "w",
            encoding: "utf8",
        });
    } else {
        stream = process.stdout;
    }

    writeHeaderIfNeeded(stream, fields, opts.append, outPath);

    // Write rows in the order of "tasks"
    let written = 0;
    for (let i = 0; i < results.length; i++) {
        const data = results[i];
        if (data && typeof data === "object") {
            stream.write(objToCsvRow(data, fields) + "\n");
            written++;
        } else if (opts.keepFailed) {
            // Write a partial row with memberId, blanks elsewhere
            const partial = {};
            for (const f of fields) partial[f] = "";
            partial.memberId = tasks[i].id;
            stream.write(objToCsvRow(partial, fields) + "\n");
            written++;
        } else {
            // Skip failed lookup
        }
    }

    if (stream !== process.stdout) {
        await new Promise((r) => stream.end(r));
        if (!opts.silent) console.error(`Closed output stream.`);
    }

    if (!opts.silent) {
        const failed = results.filter((x) => x === null).length;
        console.error(
            `Done. Successful: ${written}${opts.keepFailed ? " (including failed placeholders)" : ""
            }. Failed lookups: ${failed}.`
        );
    }

    process.exit(0);
}

// Run main
if (require.main === module) {
    main().catch((err) => {
        console.error("Fatal error:", err && err.stack ? err.stack : String(err));
        process.exit(1);
    });
}
// ---------------- End of File ----------------