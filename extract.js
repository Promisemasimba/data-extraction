#!/usr/bin/env node
"use strict";

/**
 * HMS MembershipV2 Extractor to CSV
 * Fetches all members from /api/v2/membership/members/{page}/{pageSize}
 * and writes them as CSV to a file or stdout.
 *
 * No external dependencies.
 */

const https = require("https");
const fs = require("fs");
const path = require("path");
const { URL } = require("url");

// Defaults
const DEFAULT_BASE_URL = "https://budgethealth.webportal.co.zw/api/v2";
const DEFAULT_ENDPOINT = "membership/members";
const DEFAULT_START_PAGE = 1; // Many APIs are 0-based; adjust via --start-page if needed
const DEFAULT_PAGE_SIZE = 50; // Must not exceed 50
const DEFAULT_TIMEOUT = 10000;
const DEFAULT_RETRIES = 1;

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

// ------------- Utilities -------------
function parseArgs(argv) {
    const opts = {
        baseUrl: DEFAULT_BASE_URL,
        endpoint: DEFAULT_ENDPOINT,
        scheme: process.env.BH_SCHEME || undefined,
        apiKey: process.env.BH_API_KEY || undefined,
        startPage: DEFAULT_START_PAGE,
        pageSize: DEFAULT_PAGE_SIZE,
        maxPages: undefined,
        maxItems: undefined,
        out: undefined,
        append: false,
        fields: undefined,
        timeout: DEFAULT_TIMEOUT,
        retries: DEFAULT_RETRIES,
        silent: false,
        help: false,
    };

    const args = argv.slice(2);
    for (let i = 0; i < args.length; i++) {
        const a = args[i];
        const next = () => args[i + 1];

        if (a === "-h" || a === "--help") {
            opts.help = true;
        } else if (a === "--scheme") {
            opts.scheme = next(); i++;
        } else if (a.startsWith("--scheme=")) {
            opts.scheme = a.split("=")[1];
        } else if (a === "--api-key") {
            opts.apiKey = next(); i++;
        } else if (a.startsWith("--api-key=")) {
            opts.apiKey = a.split("=")[1];
        } else if (a === "--base-url" || a === "--url") {
            opts.baseUrl = next(); i++;
        } else if (a.startsWith("--base-url=") || a.startsWith("--url=")) {
            opts.baseUrl = a.split("=")[1];
        } else if (a === "--endpoint") {
            opts.endpoint = next(); i++;
        } else if (a.startsWith("--endpoint=")) {
            opts.endpoint = a.split("=")[1];
        } else if (a === "--start-page") {
            opts.startPage = parseInt(next(), 10); i++;
        } else if (a.startsWith("--start-page=")) {
            opts.startPage = parseInt(a.split("=")[1], 10);
        } else if (a === "--page-size") {
            opts.pageSize = parseInt(next(), 10); i++;
        } else if (a.startsWith("--page-size=")) {
            opts.pageSize = parseInt(a.split("=")[1], 10);
        } else if (a === "--max-pages") {
            opts.maxPages = parseInt(next(), 10); i++;
        } else if (a.startsWith("--max-pages=")) {
            opts.maxPages = parseInt(a.split("=")[1], 10);
        } else if (a === "--max-items") {
            opts.maxItems = parseInt(next(), 10); i++;
        } else if (a.startsWith("--max-items=")) {
            opts.maxItems = parseInt(a.split("=")[1], 10);
        } else if (a === "--out") {
            opts.out = next(); i++;
        } else if (a.startsWith("--out=")) {
            opts.out = a.split("=")[1];
        } else if (a === "--append") {
            opts.append = true;
        } else if (a === "--fields") {
            opts.fields = next(); i++;
        } else if (a.startsWith("--fields=")) {
            opts.fields = a.split("=")[1];
        } else if (a === "--timeout") {
            opts.timeout = parseInt(next(), 10); i++;
        } else if (a.startsWith("--timeout=")) {
            opts.timeout = parseInt(a.split("=")[1], 10);
        } else if (a === "--retries") {
            opts.retries = parseInt(next(), 10); i++;
        } else if (a.startsWith("--retries=")) {
            opts.retries = parseInt(a.split("=")[1], 10);
        } else if (a === "--silent") {
            opts.silent = true;
        } else {
            console.error(`Unknown option: ${a}`);
            opts.help = true;
        }
    }

    if (!Number.isFinite(opts.startPage)) opts.startPage = DEFAULT_START_PAGE;
    if (!Number.isFinite(opts.pageSize)) opts.pageSize = DEFAULT_PAGE_SIZE;
    if (opts.pageSize > 50) opts.pageSize = 50;
    if (opts.pageSize <= 0) opts.pageSize = 1;
    if (opts.maxPages !== undefined && (!Number.isFinite(opts.maxPages) || opts.maxPages <= 0)) opts.maxPages = undefined;
    if (opts.maxItems !== undefined && (!Number.isFinite(opts.maxItems) || opts.maxItems <= 0)) opts.maxItems = undefined;
    if (!Number.isFinite(opts.timeout) || opts.timeout < 1000) opts.timeout = DEFAULT_TIMEOUT;
    if (!Number.isFinite(opts.retries) || opts.retries < 0) opts.retries = DEFAULT_RETRIES;

    return opts;
}

function printHelp() {
    const msg = `
HMS MembershipV2 Extractor to CSV

Usage:
  node members_to_csv.js --scheme MMSBudgetHealth --api-key <KEY> [options]

Required:
  --scheme <value>               Header "Scheme" value (or env BH_SCHEME)
  --api-key <value>              Header "x-api-key" value (or env BH_API_KEY)

Options:
  --base-url|--url <url>         Base URL (default: ${DEFAULT_BASE_URL})
  --endpoint <path>              Endpoint path (default: ${DEFAULT_ENDPOINT})
  --start-page <n>               Start page index (default: ${DEFAULT_START_PAGE})
  --page-size <n>                Page size (<= 50) (default: ${DEFAULT_PAGE_SIZE})
  --max-pages <n>                Max pages to fetch (optional)
  --max-items <n>                Max total items to fetch (optional)
  --out <file>                   Output CSV file path (default: stdout)
  --append                       Append to existing file (skip header if file has content)
  --fields <a,b,c>               CSV columns (default: all known fields)
  --timeout <ms>                 Request timeout (default: ${DEFAULT_TIMEOUT})
  --retries <n>                  Retry attempts for 429/5xx/timeouts (default: ${DEFAULT_RETRIES})
  --silent                       Reduce logs to essential errors
  -h, --help                     Show this help

Examples:
  All members to CSV file:
    node members_to_csv.js --scheme MMSBudgetHealth --api-key YOUR_KEY --out members.csv

  Start at page 0, fetch with smaller page size:
    node members_to_csv.js --scheme MMSBudgetHealth --api-key YOUR_KEY --page-size 25 --out members.csv

  Print to stdout with custom fields:
    node members_to_csv.js --scheme MMSBudgetHealth --api-key YOUR_KEY --fields memberId,memberNo,firstname,surname
`;
    console.log(msg.trim());
}

function trimSlashes(s, leading = true, trailing = true) {
    let r = s;
    if (leading) r = r.replace(/^\/+/, "");
    if (trailing) r = r.replace(/\/+$/, "");
    return r;
}

function csvEscape(value) {
    if (value === null || value === undefined) return "";
    const s = String(value);
    const needsQuotes = /[",\n\r]/.test(s);
    const escaped = s.replace(/"/g, '""');
    return needsQuotes ? `"${escaped}"` : escaped;
}

function objToCsvRow(obj, fields) {
    return fields.map((f) => csvEscape(obj && Object.prototype.hasOwnProperty.call(obj, f) ? obj[f] : "")).join(",");
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

// ------------- HTTP -------------
function makePageRequest({ baseUrl, endpoint, page, pageSize, scheme, apiKey, timeout }) {
    return new Promise((resolve) => {
        const base = trimSlashes(baseUrl, false, true);
        const ep = trimSlashes(endpoint, true, true);
        const full = `${base}/${ep}/${encodeURIComponent(page)}/${encodeURIComponent(pageSize)}`;
        const url = new URL(full);

        const options = {
            method: "GET",
            headers: {
                "Accept": "application/json",
                "Scheme": scheme,
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
                    error: parseErr ? new Error(`JSON parse error: ${parseErr.message}`) : null,
                    bodyText: data,
                    headers: res.headers,
                });
            });
        });

        req.setTimeout(timeout, () => {
            req.destroy(new Error("Request timeout"));
        });

        req.on("error", (err) => {
            resolve({ ok: false, status: 0, data: null, error: err, bodyText: "", headers: {} });
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

async function fetchPageWithRetry(params, retries, silent) {
    let attempt = 0;
    let last;
    while (attempt <= retries) {
        last = await makePageRequest(params);
        if (last.ok) return last;
        const retryable = isRetryable(last.status, last.error);
        if (!retryable || attempt === retries) break;
        const backoff = Math.min(500 * Math.pow(2, attempt), 8000);
        if (!silent) console.error(`Retrying page ${params.page} after ${last.error ? last.error.message : "HTTP " + last.status} in ${backoff}ms`);
        await new Promise((r) => setTimeout(r, backoff));
        attempt++;
    }
    return last;
}

// ------------- Main -------------
async function main() {
    const opts = parseArgs(process.argv);

    if (opts.help) {
        printHelp();
        process.exit(0);
    }

    if (!opts.scheme || !opts.apiKey) {
        console.error("Missing required headers. Provide --scheme and --api-key (or set BH_SCHEME/BH_API_KEY).");
        process.exit(1);
    }

    if (opts.pageSize > 50) {
        console.error("pageSize must not exceed 50. Adjusting to 50.");
        opts.pageSize = 50;
    }

    // Prepare output stream
    const fields = opts.fields
        ? opts.fields.split(",").map((f) => f.trim()).filter(Boolean)
        : DEFAULT_FIELDS;

    let stream;
    let outPath = null;
    if (opts.out) {
        outPath = path.resolve(process.cwd(), opts.out);
        // Ensure directory exists
        const dir = path.dirname(outPath);
        if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
        stream = fs.createWriteStream(outPath, { flags: opts.append ? "a" : "w", encoding: "utf8" });
    } else {
        stream = process.stdout;
    }

    // Write header line considering append mode
    writeHeaderIfNeeded(stream, fields, opts.append, outPath);

    const httpBase = {
        baseUrl: opts.baseUrl,
        endpoint: opts.endpoint,
        scheme: opts.scheme,
        apiKey: opts.apiKey,
        timeout: opts.timeout,
    };

    let currentPage = opts.startPage;
    let fetchedItems = 0;
    let fetchedPages = 0;

    if (!opts.silent) {
        console.error(`Starting fetch from ${trimSlashes(opts.baseUrl, false, true)}/${trimSlashes(opts.endpoint, true, true)} ...`);
        console.error(`Start page: ${opts.startPage}, page size: ${opts.pageSize}`);
    }

    try {
        while (true) {
            // Limits: max-pages
            if (opts.maxPages !== undefined && fetchedPages >= opts.maxPages) {
                if (!opts.silent) console.error(`Reached max pages limit: ${opts.maxPages}`);
                break;
            }
            // Limits: max-items
            if (opts.maxItems !== undefined && fetchedItems >= opts.maxItems) {
                if (!opts.silent) console.error(`Reached max items limit: ${opts.maxItems}`);
                break;
            }

            const res = await fetchPageWithRetry(
                { ...httpBase, page: currentPage, pageSize: opts.pageSize },
                opts.retries,
                opts.silent
            );

            if (!res.ok) {
                const snippet = (res.bodyText || "").slice(0, 200).replace(/\s+/g, " ");
                const msg = res.error ? res.error.message : `HTTP ${res.status}${snippet ? " - " + snippet : ""}`;
                console.error(`Failed to fetch page ${currentPage}: ${msg}`);
                process.exit(2);
            }

            const body = res.data;
            const results = Array.isArray(body?.results) ? body.results : [];
            const hasMore = !!body?.hasMore;

            if (!opts.silent) {
                console.error(`Page ${currentPage}: ${results.length} item(s), hasMore=${hasMore}`);
            }

            // Write rows
            for (const obj of results) {
                // Enforce max-items if specified
                if (opts.maxItems !== undefined && fetchedItems >= opts.maxItems) break;
                stream.write(objToCsvRow(obj, fields) + "\n");
                fetchedItems++;
            }

            fetchedPages++;

            // Stop conditions
            if (!hasMore) break;
            if (results.length === 0) break; // Safety guard

            currentPage++;
        }
    } finally {
        if (stream !== process.stdout) {
            await new Promise((r) => {
                stream.end(r);
            });
            if (!opts.silent) console.error(`Closed output stream.`);
        }
    }

    if (!opts.silent) {
        console.error(`Done. Pages fetched: ${fetchedPages}. Items written: ${fetchedItems}.`);
    }

    process.exit(0);
}

// Run
if (require.main === module) {
    main().catch((err) => {
        console.error("Fatal error:", err && err.stack ? err.stack : String(err));
        process.exit(1);
    });
}