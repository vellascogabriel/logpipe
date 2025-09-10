#!/usr/bin/env node

require('dontenv').config()
const { program } = require('commander')
const path = require('path')
const fs = require('fs')
const package = require('../package.json')

//Configure CLI
program
    .version(package.version)
    .description('Tool for large-scale log processing')
    .option('-i, --input <path>', 'Input log file path')
    .option('-o, --output <path>', 'Output log file path')
    .option('-f, --format <format>', 'Output log format')
    .option('-b, --batch-size <size>', 'Tamanho do lote para processamento','1000')
    .option('-w, --workers <count>', 'Number of worker processes', '1')
    .option('p, --profile', 'Enable performance profiling')
    .parse(process.argv);

const options = program.opts();

//Validate options
if(!options.input){
    console.error('Error: Input file path is required')
    program.help();
    process.exit(1);
}

//Verify if the input file exists
if(!fs.existsSync(options.input)){
    console.error('Error: Input file does not exist')
    process.exit(1);
}

//Adding principal logic(to do)

console.log('Logpipe starting with the following options:', options);
console.log('Pending implementation')

