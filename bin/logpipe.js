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
const SenderFactory = require('../src/senders/senderFactory');
const CheckpointManager = require('../src/utils/checkpointManager');
const Profiler = require('../src/utils/profiler');
const { Transform } = require('stream');
const os = require('os');

// Configure CLI
program
    .version(package.version)
    .description('Tool for large-scale log processing')
    .option('-i, --input <path>', 'Input log file path')
    .option('-o, --output <path>', 'Output log file path')
    .option('-f, --format <format>', 'File format (ndjson, csv)', 'ndjson')
    .option('-b, --batch-size <size>', 'Batch size for processing', '1000')
    .option('-w, --workers <count>', 'Number of worker processes', String(os.cpus().length))
    .option('-p, --profile', 'Enable performance profiling')
    .option('-c, --checkpoint <path>', 'Checkpoint file to resume processing')
    .option('--checkpoint-interval <ms>', 'Interval in ms to save checkpoints', '30000')
    .option('--profile-dir <path>', 'Directory to save profiling reports', './profiling')
    .option('--profile-interval <ms>', 'Interval in ms to collect profiling metrics', '5000')
    .option('--csv-separator <char>', 'CSV separator character', ',')
    .option('--csv-header <boolean>', 'CSV has header row', true)
    .option('--filter <field:value>', 'Filter records by field value')
    .option('--select <fields>', 'Select only specified fields (comma-separated)')
    .option('--count-by <field>', 'Count records by field')
    .option('--stats <keyField:valueField>', 'Calculate statistics for numeric field grouped by key')
    .option('--pretty-output', 'Format JSON output with indentation', false)
    .option('--parallel', 'Use worker threads for parallel processing', false)
    .option('--hash-field <field>', 'Calculate hash for specified field (CPU intensive)')
    .option('--enrich', 'Add processing metadata to records', false)
    // Opções para envio HTTP
    .option('--http-endpoint <url>', 'HTTP endpoint URL to send data to')
    .option('--http-method <method>', 'HTTP method (POST, PUT)', 'POST')
    .option('--http-batch-size <size>', 'Number of records to send in each HTTP request', '100')
    .option('--http-retries <count>', 'Number of retries for failed HTTP requests', '3')
    .option('--http-timeout <ms>', 'HTTP request timeout in milliseconds', '30000')
    .option('--http-headers <headers>', 'HTTP headers in JSON format')
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

// Inicializa o profiler se habilitado
const profiler = new Profiler({
    enabled: options.profile,
    outputDir: options.profileDir,
    interval: parseInt(options.profileInterval, 10)
});

// Inicializa o gerenciador de checkpoints se especificado
const checkpointManager = new CheckpointManager({
    filePath: options.checkpoint,
    interval: parseInt(options.checkpointInterval, 10),
    enabled: !!options.checkpoint
});

// Determine output destination
function createOutputSender() {
    // Se um endpoint HTTP foi especificado, cria um HttpSender
    if (options.httpEndpoint) {
        logger.info(`Output will be sent to HTTP endpoint: ${options.httpEndpoint}`);
        
        // Parse HTTP headers if provided
        let headers = {};
        if (options.httpHeaders) {
            try {
                headers = JSON.parse(options.httpHeaders);
            } catch (error) {
                logger.error(`Invalid HTTP headers JSON: ${error.message}`);
                process.exit(1);
            }
        }
        
        return SenderFactory.createHttpSender(options.httpEndpoint, {
            method: options.httpMethod,
            batchSize: parseInt(options.httpBatchSize, 10),
            retries: parseInt(options.httpRetries, 10),
            timeout: parseInt(options.httpTimeout, 10),
            headers
        });
    }
    
    // Se um arquivo de saída foi especificado, cria um FileSender
    if (options.output) {
        logger.info(`Output will be written to file: ${options.output}`);
        return SenderFactory.createFileSender(options.output, {
            encoding: 'utf8'
        });
    }
    
    // Caso contrário, usa ConsoleSender
    logger.info('Output will be written to stdout');
    return SenderFactory.createConsoleSender({
        pretty: options.prettyOutput
    });
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

// Stream de transformação para rastrear o progresso e atualizar checkpoints
class CheckpointStream extends Transform {
    constructor(options = {}) {
        super({ ...options, objectMode: true });
        this.checkpointManager = options.checkpointManager;
        this.fileSize = options.fileSize || 0;
        this.recordCount = 0;
        this.lastOffset = 0;
    }

    _transform(chunk, encoding, callback) {
        try {
            this.recordCount++;
            
            // Atualiza o checkpoint se disponível
            if (this.checkpointManager && chunk._meta && chunk._meta.offset) {
                this.lastOffset = chunk._meta.offset;
                this.checkpointManager.updateState({
                    lastProcessedOffset: chunk._meta.offset,
                    lastProcessedLine: chunk._meta.line,
                    recordsProcessed: this.recordCount
                });
            }
            
            // Passa o chunk adiante sem modificação
            this.push(chunk);
            callback();
        } catch (error) {
            callback(error);
        }
    }
}

// Função para criar transformadores com base nas opções da CLI
function createTransformers() {
    const transformers = [];
    const numWorkers = parseInt(options.workers, 10);
    
    // Verifica se deve usar worker threads para processamento paralelo
    const useWorkers = options.parallel || options.hashField;
    
    // Adiciona filtro se especificado
    if (options.filter) {
        const [field, value] = options.filter.split(':');
        if (field && value) {
            const filterCriteria = { [field]: value };
            
            // Marca o início da operação de filtro no profiler
            profiler.mark('filter');
            
            if (useWorkers) {
                // Versão com worker threads
                transformers.push(TransformerFactory.createWorkerTransformer('filter', {
                    workerData: { criteria: filterCriteria },
                    numWorkers
                }));
            } else {
                // Versão sem worker threads
                transformers.push(TransformerFactory.createFilter(filterCriteria));
            }
            
            // Marca o fim da operação de filtro no profiler
            profiler.markEnd('filter');
        }
    }
    
    // Adiciona seletor de campos se especificado
    if (options.select) {
        const fields = options.select.split(',').map(f => f.trim());
        if (fields.length > 0) {
            profiler.mark('select_fields');
            transformers.push(TransformerFactory.createFieldSelector(fields));
            profiler.markEnd('select_fields');
        }
    }
    
    // Adiciona hash se especificado (sempre usa worker threads por ser CPU intensivo)
    if (options.hashField) {
        profiler.mark('hash');
        transformers.push(TransformerFactory.createHasher(options.hashField, {
            numWorkers
        }));
        profiler.markEnd('hash');
    }
    
    // Adiciona enriquecimento se especificado
    if (options.enrich) {
        const enrichments = {
            processedAt: new Date().toISOString(),
            processedBy: 'LogPipe',
            version: package.version,
            hostname: os.hostname()
        };
        
        profiler.mark('enrich');
        
        if (useWorkers) {
            // Versão com worker threads
            transformers.push(TransformerFactory.createEnricher(enrichments, {
                numWorkers
            }));
        } else {
            // Versão sem worker threads
            transformers.push(TransformerFactory.createFieldAdder(
                Object.fromEntries(
                    Object.entries(enrichments).map(([key, value]) => [key, () => value])
                )
            ));
        }
        
        profiler.markEnd('enrich');
    }
    
    // Adiciona contador se especificado
    if (options.countBy) {
        profiler.mark('count');
        transformers.push(TransformerFactory.createCounter(options.countBy));
        profiler.markEnd('count');
    }
    
    // Adiciona estatísticas se especificado
    if (options.stats) {
        const [keyField, valueField] = options.stats.split(':');
        if (keyField && valueField) {
            profiler.mark('stats');
            transformers.push(TransformerFactory.createStats(keyField, valueField));
            profiler.markEnd('stats');
        }
    }
    
    return transformers;
}

// Função principal assíncrona
async function main() {
    try {
        // Inicia o profiler
        profiler.start();
        profiler.addEvent('process_start', { input: options.input, format: options.format });
        
        // Inicia o gerenciador de checkpoints
        checkpointManager.start();
        
        logger.info({
            input: options.input,
            format: options.format,
            batchSize: options.batchSize,
            workers: options.workers,
            parallel: options.parallel,
            httpEndpoint: options.httpEndpoint,
            checkpoint: options.checkpoint,
            profile: options.profile
        }, 'Starting LogPipe processing');

        // Verifica se deve retomar o processamento a partir de um checkpoint
        let checkpointPosition = null;
        if (checkpointManager.shouldResume()) {
            checkpointPosition = checkpointManager.getResumePosition();
            logger.info({
                offset: checkpointPosition.offset,
                line: checkpointPosition.line
            }, 'Resuming processing from checkpoint');
            
            profiler.addEvent('resume_from_checkpoint', checkpointPosition);
        }

        // Cria um stream de progresso para monitorar o processamento
        const progressStream = new ProgressStream(options.input);
        
        // Cria o parser apropriado com base no formato
        const parserOptions = {
            separator: options.csvSeparator,
            header: options.csvHeader
        };
        
        profiler.mark('parser_creation');
        const parser = createParserForFile(options.input, options.format, parserOptions);
        profiler.markEnd('parser_creation');
        
        // Cria um stream para rastrear checkpoints
        const checkpointStream = new CheckpointStream({
            checkpointManager,
            fileSize: fs.statSync(options.input).size
        });
        
        // Cria transformadores com base nas opções da CLI
        profiler.mark('transformers_creation');
        const transformers = createTransformers();
        profiler.markEnd('transformers_creation');
        
        // Determina o destino de saída (HTTP, arquivo ou console)
        profiler.mark('output_creation');
        const outputSender = createOutputSender();
        profiler.markEnd('output_creation');
        
        // Configura a pipeline de processamento completa
        let pipeline;
        
        if (options.httpEndpoint) {
            // Se estiver enviando para HTTP, não precisa do stringifier
            pipeline = [
                progressStream,
                parser,
                checkpointStream,
                ...transformers
            ];
        } else {
            // Caso contrário, adiciona o stringifier para converter objetos em JSON
            const stringifier = new JSONStringifier({ prettyOutput: options.prettyOutput });
            pipeline = [
                progressStream,
                parser,
                checkpointStream,
                ...transformers,
                stringifier
            ];
        }
        
        logger.debug({
            pipelineSteps: pipeline.length,
            transformers: transformers.map(t => t.constructor.name),
            outputType: options.httpEndpoint ? 'HTTP' : (options.output ? 'File' : 'Console')
        }, 'Processing pipeline configured');
        
        // Registra manipuladores para sinais do sistema
        setupSignalHandlers();
        
        // Processa o arquivo
        profiler.mark('file_processing');
        await processFile(options.input, outputSender, pipeline, {
            checkpoint: checkpointPosition
        });
        profiler.markEnd('file_processing');
        
        // Finaliza o processamento
        checkpointManager.stop();
        
        profiler.addEvent('process_complete');
        logger.info('Processing completed successfully');
        
        // Para o profiler e gera relatório
        profiler.stop();
    } catch (error) {
        profiler.addEvent('process_error', { error: error.message });
        logger.error({ error: error.message }, 'Error processing file');
        
        // Para o profiler e gera relatório mesmo em caso de erro
        profiler.stop();
        
        process.exit(1);
    }
}

// Configura manipuladores para sinais do sistema
function setupSignalHandlers() {
    // Tratamento de sinais para encerramento gracioso
    process.on('SIGINT', () => {
        logger.info('Received SIGINT signal. Shutting down...');
        checkpointManager.stop();
        profiler.addEvent('process_interrupted', { signal: 'SIGINT' });
        profiler.stop();
        process.exit(0);
    });

    process.on('SIGTERM', () => {
        logger.info('Received SIGTERM signal. Shutting down...');
        checkpointManager.stop();
        profiler.addEvent('process_interrupted', { signal: 'SIGTERM' });
        profiler.stop();
        process.exit(0);
    });
}

// Inicia o processamento
main();
