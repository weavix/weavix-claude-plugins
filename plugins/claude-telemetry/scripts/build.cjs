#!/usr/bin/env node
'use strict';

/**
 * Bundles src/parquet-writer.js and its parquet dependency into dist/parquet-writer.cjs.
 * `--check` rebuilds to a temp file and fails if the committed bundle is stale.
 */

const fs = require('fs');
const os = require('os');
const path = require('path');
const esbuild = require('esbuild');

const root = path.join(__dirname, '..');
const target = path.join(root, 'dist', 'parquet-writer.cjs');
const check = process.argv.includes('--check');
const outfile = check ? path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'pw-')), 'parquet-writer.cjs') : target;

esbuild.buildSync({
    entryPoints: [path.join(root, 'src', 'parquet-writer.js')],
    bundle: true,
    platform: 'node',
    target: 'node18',
    format: 'cjs',
    minify: true,
    legalComments: 'external',
    outfile,
});

if (check) {
    const same = fs.readFileSync(outfile).equals(fs.readFileSync(target));
    if (!same) {
        console.error('dist/parquet-writer.cjs is stale: run `npm run build` and commit the result.');
        process.exit(1);
    }
    console.log('dist/parquet-writer.cjs is up to date.');
}
