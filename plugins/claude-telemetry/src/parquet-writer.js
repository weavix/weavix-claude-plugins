'use strict';

/**
 * Bundle entry for dist/parquet-writer.cjs: encodes spooled telemetry rows as one parquet file.
 * Bundled with @dsnp/parquetjs so the plugin needs no install; loaded only when a batch is flushed.
 */

const { Writable } = require('stream');
const { ParquetSchema, ParquetWriter } = require('@dsnp/parquetjs');

/**
 * Build the parquet schema for the given column list.
 * @param {string[]} columns column names, in row order
 * @param {Record<string, string>} types parquet type per column; unlisted columns are UTF8
 */
function buildSchema(columns, types) {
    const fields = {};
    for (const c of columns) {
        fields[c] = { type: types[c] || 'UTF8', optional: true, compression: 'SNAPPY' };
    }
    return new ParquetSchema(fields);
}

/**
 * Coerce a spooled value to what its parquet type accepts; anything unusable becomes null.
 * @param {unknown} v value from the spool
 * @param {string} type parquet type of its column
 */
function coerce(v, type) {
    if (v === null || v === undefined) return null;
    if (type === 'INT64') {
        const n = Number(v);
        return Number.isFinite(n) ? BigInt(Math.trunc(n)) : null;
    }
    if (type === 'BOOLEAN') return typeof v === 'boolean' ? v : /^(1|true)$/i.test(String(v));
    if (type === 'TIMESTAMP_MILLIS') {
        const d = v instanceof Date ? v : new Date(v);
        return Number.isNaN(d.getTime()) ? null : d;
    }
    return String(v);
}

/**
 * Encode rows as a single parquet file held in memory.
 * @param {Array<Array<unknown>>} rows values in `columns` order
 * @param {string[]} columns column names
 * @param {Record<string, string>} types parquet type per column (INT64, BOOLEAN, TIMESTAMP_MILLIS; default UTF8)
 * @returns {Promise<Buffer>}
 */
async function encodeParquet(rows, columns, types) {
    const parts = [];
    const sink = new Writable({
        write(chunk, _enc, cb) { parts.push(Buffer.from(chunk)); cb(); },
    });
    const writer = await ParquetWriter.openStream(buildSchema(columns, types), sink);
    try {
        for (const row of rows) {
            const record = {};
            columns.forEach((c, i) => {
                const v = coerce(row[i], types[c] || 'UTF8');
                if (v !== null) record[c] = v;
            });
            await writer.appendRow(record);
        }
    } finally {
        await writer.close();
    }
    return Buffer.concat(parts);
}

module.exports = { encodeParquet };
