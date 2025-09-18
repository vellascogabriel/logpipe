#!/usr/bin/env node

require('dotenv').config();
const { program } = require('commander');
const path = require('path');
const fs = require('fs');
const package = require('../package.json');
const { createReadStream, processFile } = require('../src/readers/fileReader');
const ProgressStream = require('../src/utils/progressStream');
const logger = require('../src/utils/logger');
const { createParserForFile } = require('../src/parsers/parserFactory');
const TransformerFactory = require('../src/transformers/transformerFactory');
const { Transform } = require('stream');

// Configure CLI
program
    .version(package.version)
    .description('Tool for large-scale log processing')
    .option('-i, --input <path>', 'Input log file path')
    .option('-o, --output <path>', 'Output log file path')
    .option('-f, --format <format>', 'File format (ndjson, csv)', 'ndjson')
    .option('-b, --batch-size <size>', 'Batch size for processing', '1000')
    .option('-w, --workers <count>', 'Number of worker processes', '1')
    .option('-p, --profile', 'Enable performance profiling')
    .option('-c, --checkpoint <path>', 'Checkpoint file to resume processing')
    .option('--csv-separator <char>', 'CSV separator character', ',')
    .option('--csv-header <boolean>', 'CSV has header row', true)
    .option('--filter <field:value>', 'Filter records by field value')
    .option('--select <fields>', 'Select only specified fields (comma-separated)')
    .option('--count-by <field>', 'Count records by field')
    .option('--stats <keyField:valueField>', 'Calculate statistics for numeric field grouped by key')
    .option('--pretty-output', 'Format JSON output with indentation', false)
    .parse(process.argv);

const options = program.opts();

// Validate options
if(!options.input){
    logger.error('Input file path is required');
    program.help();
    process.exit(1);
}

// Verify if the input file exists
if(!fs.existsSync(options.input)){
    logger.error(`Input file does not exist: ${options.input}`);
    process.exit(1);
}

// Determine output destination
let outputStream;
if (options.output) {
    // Se um arquivo de saída foi especificado, cria um stream de escrita
    outputStream = fs.createWriteStream(options.output);
    logger.info(`Output will be written to: ${options.output}`);
} else {
    // Caso contrário, usa stdout
    outputStream = process.stdout;
    logger.info('Output will be written to stdout');
}

// Stream de transformação para serializar objetos de volta para JSON
class JSONStringifier extends Transform {
    constructor(options = {}) {
        super({ ...options, objectMode: true });
        this.prettyOutput = options.prettyOutput || false;
    }

    _transform(chunk, encoding, callback) {
        try {
            // Converte o objeto para string JSON com ou sem formatação
            const jsonString = this.prettyOutput 
                ? JSON.stringify(chunk, null, 2) + '\n'
                : JSON.stringify(chunk) + '\n';
            this.push(jsonString);
            callback();
        } catch (error) {
            callback(error);
        }
    }
}

// Função para criar transformadores com base nas opções da CLI
function createTransformers() {
    const transformers = [];
    
    // Adiciona filtro se especificado
    if (options.filter) {
        const [field, value] = options.filter.split(':');
        if (field && value) {
            const filterCriteria = { [field]: value };
            transformers.push(TransformerFactory.createFilter(filterCriteria));
        }
    }
    
    // Adiciona seletor de campos se especificado
    if (options.select) {
        const fields = options.select.split(',').map(f => f.trim());
        if (fields.length > 0) {
            transformers.push(TransformerFactory.createFieldSelector(fields));
        }
    }
    
    // Adiciona contador se especificado
    if (options.countBy) {
        transformers.push(TransformerFactory.createCounter(options.countBy));
    }
    
    // Adiciona estatísticas se especificado
    if (options.stats) {
        const [keyField, valueField] = options.stats.split(':');
        if (keyField && valueField) {
            transformers.push(TransformerFactory.createStats(keyField, valueField));
        }
    }
    
    return transformers;
}

// Função principal assíncrona
async function main() {
    try {
        logger.info({
            input: options.input,
            format: options.format,
            batchSize: options.batchSize,
            workers: options.workers
        }, 'Starting LogPipe processing');

        // Cria um stream de progresso para monitorar o processamento
        const progressStream = new ProgressStream(options.input);
        
        // Cria o parser apropriado com base no formato
        const parserOptions = {
            separator: options.csvSeparator,
            header: options.csvHeader
        };
        const parser = createParserForFile(options.input, options.format, parserOptions);
        
        // Cria transformadores com base nas opções da CLI
        const transformers = createTransformers();
        
        // Cria um stream para serializar objetos de volta para JSON
        const stringifier = new JSONStringifier({ prettyOutput: options.prettyOutput });
        
        // Configura a pipeline de processamento completa:
        // arquivo -> descompressão -> progresso -> parser -> transformadores -> stringifier -> saída
        const pipeline = [
            progressStream,
            parser,
            ...transformers,
            stringifier
        ];
        
        logger.debug({
            pipelineSteps: pipeline.length,
            transformers: transformers.map(t => t.constructor.name)
        }, 'Processing pipeline configured');
        
        await processFile(options.input, outputStream, pipeline);
        
        logger.info('Processing completed successfully');
    } catch (error) {
        logger.error({ error: error.message }, 'Error processing file');
        process.exit(1);
    }
}

// Tratamento de sinais para encerramento gracioso
process.on('SIGINT', () => {
    logger.info('Received SIGINT signal. Shutting down...');
    // Nos próximos passos, implementaremos salvamento de checkpoints aqui
    process.exit(0);
});

process.on('SIGTERM', () => {
    logger.info('Received SIGTERM signal. Shutting down...');
    process.exit(0);
});

// Inicia o processamento
main();
