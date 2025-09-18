const { Transform } = require('stream');
const logger = require('../utils/logger');

/**
 * Classe base para todos os transformadores
 * Implementa funcionalidades comuns e define a interface padrão
 */
class BaseTransformer extends Transform {
  /**
   * Cria uma nova instância do transformador base
   * @param {Object} options - Opções do transformador
   */
  constructor(options = {}) {
    // Todos os transformadores trabalham com objetos
    super({ ...options, objectMode: true });
    
    // Nome do transformador para logging
    this.name = options.name || this.constructor.name;
    
    // Contador de registros processados
    this.processedCount = 0;
    
    // Contador de registros transformados (modificados, filtrados, etc.)
    this.transformedCount = 0;
    
    // Intervalo para logging de estatísticas
    this.statsInterval = options.statsInterval || 10000;
    this.lastStatsTime = Date.now();
    
    // Registra evento para logging final quando o stream terminar
    this.on('end', () => {
      this.logFinalStats();
    });
  }

  /**
   * Implementação do método _transform exigido pela interface Transform
   * @param {Object} record - Registro a ser transformado
   * @param {string} encoding - Codificação (ignorada em modo de objeto)
   * @param {function} callback - Função de callback
   */
  _transform(record, encoding, callback) {
    try {
      this.processedCount++;
      
      // Chama o método de transformação específico da subclasse
      this.transformRecord(record, (error, transformedRecord) => {
        if (error) {
          return callback(error);
        }
        
        // Se transformedRecord for null/undefined, o registro é filtrado
        if (transformedRecord) {
          this.transformedCount++;
          this.push(transformedRecord);
        }
        
        // Loga estatísticas periodicamente
        this.logPeriodicStats();
        
        callback();
      });
    } catch (error) {
      logger.error({ 
        transformer: this.name, 
        error: error.message,
        record
      }, 'Error in transformer');
      callback(error);
    }
  }

  /**
   * Método a ser implementado pelas subclasses para transformar registros
   * @param {Object} record - Registro a ser transformado
   * @param {function} callback - Função de callback(error, transformedRecord)
   */
  transformRecord(record, callback) {
    // Implementação padrão: passa o registro sem modificação
    callback(null, record);
  }

  /**
   * Loga estatísticas periódicas
   */
  logPeriodicStats() {
    const now = Date.now();
    if (now - this.lastStatsTime >= this.statsInterval) {
      logger.debug({
        transformer: this.name,
        processed: this.processedCount,
        transformed: this.transformedCount
      }, 'Transformer progress');
      this.lastStatsTime = now;
    }
  }

  /**
   * Loga estatísticas finais
   */
  logFinalStats() {
    logger.info({
      transformer: this.name,
      processed: this.processedCount,
      transformed: this.transformedCount,
      filteredOut: this.processedCount - this.transformedCount
    }, 'Transformer completed');
  }
}

module.exports = BaseTransformer;
