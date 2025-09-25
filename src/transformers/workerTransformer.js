const BaseTransformer = require('./baseTransformer');
const WorkerPool = require('../workers/workerPool');
const path = require('path');
const logger = require('../utils/logger');
const os = require('os');

/**
 * Transformador que utiliza worker threads para processamento paralelo
 */
class WorkerTransformer extends BaseTransformer {
  /**
   * Cria uma nova instância do transformador com worker threads
   * @param {Object} options - Opções do transformador
   * @param {string} options.taskType - Tipo de tarefa a ser executada pelos workers
   * @param {number} options.numWorkers - Número de workers (padrão: número de CPUs)
   * @param {string} options.workerScript - Caminho para o script do worker (opcional)
   * @param {Object} options.workerData - Dados adicionais para os workers (opcional)
   * @param {number} options.batchSize - Tamanho do lote para processamento em batch (opcional)
   */
  constructor(options = {}) {
    super(options);
    
    if (!options.taskType) {
      throw new Error('A opção taskType é obrigatória para o WorkerTransformer');
    }
    
    this.taskType = options.taskType;
    this.numWorkers = options.numWorkers || os.cpus().length;
    this.workerScript = options.workerScript || path.join(__dirname, '../workers/worker.js');
    this.workerData = options.workerData || {};
    this.batchSize = options.batchSize || 1; // Padrão: processar um registro por vez
    
    // Flag para indicar se o pool de workers foi inicializado
    this.initialized = false;
    
    // Fila de registros para processamento em batch
    this.recordBatch = [];
    
    // Contador de registros em processamento
    this.pendingRecords = 0;
    
    logger.info({
      transformer: this.name,
      taskType: this.taskType,
      numWorkers: this.numWorkers,
      batchSize: this.batchSize
    }, 'Worker transformer created');
  }

  /**
   * Inicializa o pool de workers
   * @returns {Promise<void>} - Promise resolvida quando o pool estiver pronto
   */
  async initWorkerPool() {
    if (this.initialized) {
      return;
    }
    
    logger.info('Initializing worker pool');
    
    this.workerPool = new WorkerPool({
      numWorkers: this.numWorkers,
      workerScript: this.workerScript,
      workerData: {
        ...this.workerData,
        taskType: this.taskType
      }
    });
    
    // Configura listener para estatísticas dos workers
    this.workerPool.on('worker:stats', ({ stats }) => {
      logger.debug({
        transformer: this.name,
        workerStats: stats
      }, 'Worker stats received');
    });
    
    await this.workerPool.init();
    this.initialized = true;
    
    logger.info('Worker pool initialized successfully');
  }

  /**
   * Implementação do método _transform exigido pela interface Transform
   * @param {Object} record - Registro a ser transformado
   * @param {string} encoding - Codificação (ignorada em modo de objeto)
   * @param {function} callback - Função de callback
   */
  async _transform(record, encoding, callback) {
    try {
      // Inicializa o pool de workers se necessário
      if (!this.initialized) {
        await this.initWorkerPool();
      }
      
      this.processedCount++;
      
      if (this.batchSize > 1) {
        // Modo de processamento em batch
        this.recordBatch.push({ record, callback });
        
        // Se atingiu o tamanho do lote, processa o batch
        if (this.recordBatch.length >= this.batchSize) {
          await this.processBatch();
        } else {
          // Caso contrário, apenas retorna sem chamar o callback
          // O callback será chamado quando o batch for processado
          return;
        }
      } else {
        // Modo de processamento individual
        await this.processRecord(record, callback);
      }
    } catch (error) {
      logger.error({
        transformer: this.name,
        error: error.message
      }, 'Error in worker transformer');
      callback(error);
    }
  }

  /**
   * Processa um registro individual
   * @param {Object} record - Registro a ser processado
   * @param {function} callback - Função de callback
   * @returns {Promise<void>} - Promise resolvida quando o registro for processado
   */
  async processRecord(record, callback) {
    try {
      this.pendingRecords++;
      
      // Submete a tarefa para o pool de workers
      const result = await this.workerPool.submitTask({
        type: this.taskType,
        data: {
          record,
          options: this.workerData
        }
      });
      
      this.pendingRecords--;
      
      // Se o resultado for null/undefined, filtra o registro
      if (result === null || result === undefined) {
        callback();
      } else {
        this.transformedCount++;
        this.push(result);
        callback();
      }
    } catch (error) {
      this.pendingRecords--;
      logger.error({
        transformer: this.name,
        error: error.message,
        record
      }, 'Error processing record in worker');
      callback(error);
    }
  }

  /**
   * Processa um lote de registros
   * @returns {Promise<void>} - Promise resolvida quando o lote for processado
   */
  async processBatch() {
    if (this.recordBatch.length === 0) {
      return;
    }
    
    const batch = [...this.recordBatch];
    this.recordBatch = [];
    
    try {
      // Extrai os registros e callbacks
      const records = batch.map(item => item.record);
      const callbacks = batch.map(item => item.callback);
      
      this.pendingRecords += records.length;
      
      // Submete a tarefa de batch para o pool de workers
      const results = await this.workerPool.submitTask({
        type: this.taskType,
        data: {
          records,
          isBatch: true,
          options: this.workerData
        }
      });
      
      this.pendingRecords -= records.length;
      
      // Processa os resultados
      if (Array.isArray(results)) {
        for (let i = 0; i < results.length; i++) {
          const result = results[i];
          const callback = callbacks[i];
          
          // Se o resultado for null/undefined, filtra o registro
          if (result === null || result === undefined) {
            callback();
          } else {
            this.transformedCount++;
            this.push(result);
            callback();
          }
        }
      } else {
        // Se o resultado não for um array, algo deu errado
        throw new Error('Expected array of results from batch processing');
      }
    } catch (error) {
      this.pendingRecords -= batch.length;
      logger.error({
        transformer: this.name,
        error: error.message,
        batchSize: batch.length
      }, 'Error processing batch in worker');
      
      // Chama todos os callbacks com erro
      batch.forEach(item => item.callback(error));
    }
  }

  /**
   * Implementação do método _flush exigido pela interface Transform
   * @param {function} callback - Função de callback
   */
  async _flush(callback) {
    try {
      // Processa qualquer registro restante no batch
      if (this.recordBatch.length > 0) {
        await this.processBatch();
      }
      
      // Aguarda todos os registros pendentes serem processados
      if (this.pendingRecords > 0) {
        logger.info(`Waiting for ${this.pendingRecords} pending records to complete`);
        
        await new Promise(resolve => {
          const checkPending = () => {
            if (this.pendingRecords === 0) {
              resolve();
            } else {
              setTimeout(checkPending, 100);
            }
          };
          checkPending();
        });
      }
      
      // Encerra o pool de workers se foi inicializado
      if (this.initialized && this.workerPool) {
        logger.info('Shutting down worker pool');
        await this.workerPool.shutdown();
      }
      
      this.logFinalStats();
      callback();
    } catch (error) {
      logger.error({
        transformer: this.name,
        error: error.message
      }, 'Error in worker transformer flush');
      callback(error);
    }
  }
}

/**
 * Cria um transformador que utiliza worker threads para processamento paralelo
 * @param {string} taskType - Tipo de tarefa a ser executada pelos workers
 * @param {Object} options - Opções adicionais
 * @returns {WorkerTransformer} - Instância do transformador
 */
function createWorkerTransformer(taskType, options = {}) {
  return new WorkerTransformer({
    ...options,
    taskType
  });
}

module.exports = {
  WorkerTransformer,
  createWorkerTransformer
};
