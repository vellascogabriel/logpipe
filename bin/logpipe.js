#!/usr/bin/env node

require('dontenv').config()
const { program } = require('commander')
const path = require('path')
const fs = require('fs')
const package = require('../package.json')
const { createReadStream, processFile } = require('../readers/fileReader')
const ProgressStream = require('../src/utils/progressStream')
const logger = require('../src/utils/logger')

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

let outputStream;

if(options.output){
    outputStream = fs.createWriteStream(options.output);
    logger.info(`Output will be written to ${options.output}`);
} else {
    outputStream = process.stdout;
    logger.info('Output will be written to stdout');
}


async function main() {
    try {

        logger.info({
            input: options.input,
            format: options.format,
            batchSize: options.batchSize,
            workers: options.workers
        }, 'Starting Logpipe processing')

        const progressStream = new ProgressStream(options.input);

        await processFile(options.input, outputStream, [progressStream]);
        logger.info('Logpipe processing completed');
    } catch (error) {
        logger.error('Error starting Logpipe processing', error);
        process.exit(1);
    }
}

process.on('SIGINT', () => {
    logger.info('Logpipe process interrupted by user');
    process.exit(0);
})

process.on('SIGTERM', () => {
    logger.info('Logpipe process interrupted by user');
    process.exit(0);
})

main();


