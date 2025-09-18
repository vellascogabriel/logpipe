const { Writable } = require('stream');
const http = require('http');
const https = require('https');
const { URL } = require('url');
const logger = require('../utils/logger');

/**
 * Stream de escrita que envia dados em lotes para um servidor HTTP
 */
class HttpSender extends Writable {
  /**
   * Cria uma nova instância do HttpSender
   * @param {Object} options - Opções de configuração
   * @param {string} options.endpoint - URL do endpoint HTTP
   * @param {number} options.batchSize - Tamanho do lote (número de registros)
   * @param {number} options.timeout - Timeout da requisição em ms
   * @param {number} options.retries - Número máximo de tentativas em caso de falha
   * @param {number} options.retryDelay - Tempo de espera entre tentativas em ms
   * @param {Object} options.headers - Cabeçalhos HTTP adicionais
   * @param {string} options.method - Método HTTP (POST, PUT)
   */
  constructor(options = {}) {
    // Configura o modo de objeto para receber objetos JavaScript
    super({ objectMode: true });
    
    if (!options.endpoint) {
      throw new Error('Endpoint URL is required');
    }
    
    this.endpoint = options.endpoint;
    this.batchSize = options.batchSize || 100;
    this.timeout = options.timeout || 30000; // 30 segundos por padrão
    this.retries = options.retries || 3;
    this.retryDelay = options.retryDelay || 1000; // 1 segundo por padrão
    this.headers = options.headers || {};
    this.method = (options.method || 'POST').toUpperCase();
    
    // Verifica se o método é suportado
    if (!['POST', 'PUT'].includes(this.method)) {
      throw new Error(`Unsupported HTTP method: ${this.method}`);
    }
    
    // Analisa a URL para determinar o protocolo
    this.url = new URL(this.endpoint);
    this.httpModule = this.url.protocol === 'https:' ? https : http;
    
    // Buffer para acumular registros até atingir o tamanho do lote
    this.batch = [];
    
    // Estatísticas
    this.stats = {
      recordsReceived: 0,
      recordsSent: 0,
      batchesSent: 0,
      batchesFailed: 0,
      retriesPerformed: 0,
      totalSendTime: 0
    };
    
    // Intervalo para logging periódico de estatísticas
    this.statsInterval = setInterval(() => this.logStats(), 60000); // A cada minuto
    
    logger.info({
      endpoint: this.endpoint,
      batchSize: this.batchSize,
      method: this.method
    }, 'HTTP sender initialized');
  }

  /**
   * Implementação do método _write exigido pela interface Writable
   * @param {Object} record - Registro a ser enviado
   * @param {string} encoding - Codificação (ignorada em modo de objeto)
   * @param {function} callback - Função de callback
   */
  _write(record, encoding, callback) {
    try {
      // Adiciona o registro ao lote atual
      this.batch.push(record);
      this.stats.recordsReceived++;
      
      // Se atingiu o tamanho do lote, envia
      if (this.batch.length >= this.batchSize) {
        this.sendBatch()
          .then(() => callback())
          .catch(err => callback(err));
      } else {
        // Caso contrário, apenas continua
        callback();
      }
    } catch (error) {
      logger.error({ error: error.message }, 'Error in HTTP sender _write');
      callback(error);
    }
  }

  /**
   * Implementação do método _writev para processamento em bulk
   * @param {Array} chunks - Array de chunks a serem escritos
   * @param {function} callback - Função de callback
   */
  _writev(chunks, callback) {
    try {
      // Adiciona todos os registros ao lote atual
      for (const { chunk } of chunks) {
        this.batch.push(chunk);
        this.stats.recordsReceived++;
      }
      
      // Se atingiu o tamanho do lote, envia
      if (this.batch.length >= this.batchSize) {
        this.sendBatch()
          .then(() => callback())
          .catch(err => callback(err));
      } else {
        // Caso contrário, apenas continua
        callback();
      }
    } catch (error) {
      logger.error({ error: error.message }, 'Error in HTTP sender _writev');
      callback(error);
    }
  }

  /**
   * Implementação do método _final exigido pela interface Writable
   * @param {function} callback - Função de callback
   */
  _final(callback) {
    // Envia qualquer registro restante no lote
    if (this.batch.length > 0) {
      this.sendBatch()
        .then(() => {
          this.cleanup();
          callback();
        })
        .catch(err => {
          this.cleanup();
          callback(err);
        });
    } else {
      this.cleanup();
      callback();
    }
  }

  /**
   * Limpa recursos
   */
  cleanup() {
    clearInterval(this.statsInterval);
    this.logStats(true); // Log final de estatísticas
  }

  /**
   * Envia o lote atual para o servidor HTTP
   * @returns {Promise<void>} - Promise resolvida quando o lote for enviado com sucesso
   */
  async sendBatch() {
    if (this.batch.length === 0) {
      return;
    }
    
    const batchToSend = [...this.batch];
    this.batch = [];
    
    logger.debug({
      endpoint: this.endpoint,
      batchSize: batchToSend.length
    }, 'Sending batch to HTTP endpoint');
    
    let attempt = 0;
    let success = false;
    let lastError;
    
    const startTime = Date.now();
    
    while (attempt < this.retries && !success) {
      attempt++;
      
      try {
        await this.sendRequest(batchToSend);
        success = true;
        
        const sendTime = Date.now() - startTime;
        this.stats.totalSendTime += sendTime;
        this.stats.recordsSent += batchToSend.length;
        this.stats.batchesSent++;
        
        logger.debug({
          endpoint: this.endpoint,
          batchSize: batchToSend.length,
          attempt,
          sendTime
        }, 'Batch sent successfully');
      } catch (error) {
        lastError = error;
        this.stats.retriesPerformed++;
        
        logger.warn({
          endpoint: this.endpoint,
          batchSize: batchToSend.length,
          attempt,
          error: error.message,
          statusCode: error.statusCode
        }, 'Failed to send batch, retrying');
        
        // Espera antes de tentar novamente (exponential backoff)
        if (attempt < this.retries) {
          const delay = this.retryDelay * Math.pow(2, attempt - 1);
          await new Promise(resolve => setTimeout(resolve, delay));
        }
      }
    }
    
    if (!success) {
      this.stats.batchesFailed++;
      
      const error = new Error(`Failed to send batch after ${this.retries} attempts`);
      error.originalError = lastError;
      
      logger.error({
        endpoint: this.endpoint,
        batchSize: batchToSend.length,
        attempts: this.retries,
        error: lastError.message,
        statusCode: lastError.statusCode
      }, 'Batch sending failed permanently');
      
      throw error;
    }
  }

  /**
   * Envia uma requisição HTTP
   * @param {Array} batch - Lote de registros a ser enviado
   * @returns {Promise<void>} - Promise resolvida quando a requisição for concluída com sucesso
   */
  sendRequest(batch) {
    return new Promise((resolve, reject) => {
      // Prepara o corpo da requisição
      const body = JSON.stringify(batch);
      
      // Prepara as opções da requisição
      const options = {
        method: this.method,
        headers: {
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(body),
          ...this.headers
        },
        timeout: this.timeout
      };
      
      // Cria a requisição
      const req = this.httpModule.request(this.endpoint, options, (res) => {
        let responseData = '';
        
        res.on('data', (chunk) => {
          responseData += chunk;
        });
        
        res.on('end', () => {
          if (res.statusCode >= 200 && res.statusCode < 300) {
            // Sucesso
            resolve();
          } else {
            // Erro HTTP
            const error = new Error(`HTTP error: ${res.statusCode}`);
            error.statusCode = res.statusCode;
            error.responseData = responseData;
            reject(error);
          }
        });
      });
      
      // Configura handlers de erro
      req.on('error', (error) => {
        reject(error);
      });
      
      req.on('timeout', () => {
        req.destroy();
        const error = new Error(`Request timeout after ${this.timeout}ms`);
        error.code = 'ETIMEDOUT';
        reject(error);
      });
      
      // Envia o corpo da requisição
      req.write(body);
      req.end();
    });
  }

  /**
   * Loga estatísticas de envio
   * @param {boolean} isFinal - Se true, indica que é o log final
   */
  logStats(isFinal = false) {
    const avgSendTime = this.stats.batchesSent > 0
      ? this.stats.totalSendTime / this.stats.batchesSent
      : 0;
    
    const stats = {
      ...this.stats,
      avgSendTime,
      pendingRecords: this.batch.length,
      successRate: this.stats.recordsReceived > 0
        ? (this.stats.recordsSent / this.stats.recordsReceived) * 100
        : 0
    };
    
    if (isFinal) {
      logger.info({ stats, endpoint: this.endpoint }, 'HTTP sender final statistics');
    } else {
      logger.debug({ stats, endpoint: this.endpoint }, 'HTTP sender statistics');
    }
  }
}

module.exports = HttpSender;
