const BaseTransformer = require('./baseTransformer');
const logger = require('../utils/logger');

/**
 * Transformador que agrega registros com base em uma chave e função de agregação
 */
class AggregateTransformer extends BaseTransformer {
  /**
   * Cria uma nova instância do transformador de agregação
   * @param {Object} options - Opções do transformador
   * @param {Function|string} options.keyFn - Função ou nome de campo para extrair a chave de agregação
   * @param {Function} options.aggregateFn - Função para agregar registros
   * @param {number} options.flushInterval - Intervalo em ms para emitir agregações parciais
   * @param {number} options.maxGroups - Número máximo de grupos a manter em memória
   */
  constructor(options = {}) {
    super(options);
    
    // Função para extrair a chave de agregação
    if (typeof options.keyFn === 'function') {
      this.keyFn = options.keyFn;
    } else if (typeof options.keyFn === 'string') {
      // Se for uma string, usa-a como nome de campo
      const fieldName = options.keyFn;
      this.keyFn = (record) => {
        return record[fieldName];
      };
    } else {
      throw new Error('A opção keyFn deve ser uma função ou uma string');
    }
    
    // Função de agregação
    if (typeof options.aggregateFn !== 'function') {
      throw new Error('A opção aggregateFn deve ser uma função');
    }
    this.aggregateFn = options.aggregateFn;
    
    // Configurações de flush
    this.flushInterval = options.flushInterval || 30000; // 30 segundos por padrão
    this.maxGroups = options.maxGroups || 1000;
    this.lastFlushTime = Date.now();
    
    // Armazena as agregações em andamento
    this.groups = new Map();
    
    // Contador de registros por grupo
    this.recordsPerGroup = new Map();
    
    // Registra evento para flush final quando o stream terminar
    this.on('end', () => {
      this.flushAll();
    });
    
    logger.debug({
      transformer: this.name,
      flushInterval: this.flushInterval,
      maxGroups: this.maxGroups
    }, 'Aggregate transformer initialized');
  }

  /**
   * Transforma (agrega) um registro
   * @param {Object} record - Registro a ser agregado
   * @param {function} callback - Função de callback
   */
  transformRecord(record, callback) {
    try {
      // Extrai a chave de agregação
      const key = this.keyFn(record);
      
      // Se a chave for null/undefined, passa o registro sem agregação
      if (key === null || key === undefined) {
        return callback(null, record);
      }
      
      // Converte a chave para string para usar como chave do Map
      const keyStr = String(key);
      
      // Obtém ou cria o grupo para esta chave
      if (!this.groups.has(keyStr)) {
        this.groups.set(keyStr, record);
        this.recordsPerGroup.set(keyStr, 1);
      } else {
        // Agrega o registro ao grupo existente
        const currentGroup = this.groups.get(keyStr);
        const newGroup = this.aggregateFn(currentGroup, record);
        this.groups.set(keyStr, newGroup);
        this.recordsPerGroup.set(keyStr, this.recordsPerGroup.get(keyStr) + 1);
      }
      
      // Verifica se deve fazer flush
      const now = Date.now();
      if (now - this.lastFlushTime >= this.flushInterval || this.groups.size >= this.maxGroups) {
        this.flushAll();
        this.lastFlushTime = now;
      }
      
      // Não emite nada aqui, os registros agregados serão emitidos no flush
      callback();
    } catch (error) {
      logger.error({
        transformer: this.name,
        error: error.message,
        record
      }, 'Error aggregating record');
      callback(error);
    }
  }

  /**
   * Emite todos os grupos agregados e limpa o estado
   */
  flushAll() {
    logger.debug({
      transformer: this.name,
      groupCount: this.groups.size
    }, 'Flushing aggregated groups');
    
    // Emite cada grupo agregado
    for (const [key, group] of this.groups.entries()) {
      // Adiciona metadados de agregação ao grupo
      const enrichedGroup = {
        ...group,
        _aggregation: {
          key,
          count: this.recordsPerGroup.get(key),
          flushTime: new Date().toISOString()
        }
      };
      
      this.push(enrichedGroup);
    }
    
    // Limpa os grupos
    this.groups.clear();
    this.recordsPerGroup.clear();
  }

  /**
   * Implementação do método _flush exigido pela interface Transform
   * @param {function} callback - Função de callback
   */
  _flush(callback) {
    try {
      // Emite quaisquer grupos restantes
      this.flushAll();
      callback();
    } catch (error) {
      logger.error({
        transformer: this.name,
        error: error.message
      }, 'Error in aggregate flush');
      callback(error);
    }
  }
}

/**
 * Função auxiliar para criar um agregador de contagem
 * @param {string|Function} keyField - Campo ou função para extrair a chave
 * @param {Object} options - Opções adicionais
 * @returns {AggregateTransformer} - Instância do transformador
 */
function createCounter(keyField, options = {}) {
  return new AggregateTransformer({
    ...options,
    name: 'Counter',
    keyFn: keyField,
    aggregateFn: (currentGroup, newRecord) => {
      return {
        ...currentGroup,
        count: (currentGroup.count || 1) + 1
      };
    }
  });
}

/**
 * Função auxiliar para criar um agregador de estatísticas numéricas
 * @param {string|Function} keyField - Campo ou função para extrair a chave
 * @param {string} valueField - Campo contendo o valor numérico
 * @param {Object} options - Opções adicionais
 * @returns {AggregateTransformer} - Instância do transformador
 */
function createStats(keyField, valueField, options = {}) {
  return new AggregateTransformer({
    ...options,
    name: 'Stats',
    keyFn: keyField,
    aggregateFn: (currentGroup, newRecord) => {
      const value = Number(newRecord[valueField]);
      
      if (isNaN(value)) {
        return currentGroup;
      }
      
      const count = (currentGroup.count || 1) + 1;
      const sum = (currentGroup.sum || currentGroup[valueField]) + value;
      const min = Math.min(currentGroup.min !== undefined ? currentGroup.min : currentGroup[valueField], value);
      const max = Math.max(currentGroup.max !== undefined ? currentGroup.max : currentGroup[valueField], value);
      const avg = sum / count;
      
      return {
        ...currentGroup,
        count,
        sum,
        min,
        max,
        avg
      };
    }
  });
}

module.exports = {
  AggregateTransformer,
  createCounter,
  createStats
};
