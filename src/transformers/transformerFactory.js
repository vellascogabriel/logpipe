const FilterTransformer = require('./filterTransformer');
const { MapTransformer, createMapper, createFieldSelector } = require('./mapTransformer');
const { AggregateTransformer, createCounter, createStats } = require('./aggregateTransformer');
const logger = require('../utils/logger');

/**
 * Fábrica para criar e compor transformadores
 */
class TransformerFactory {
  /**
   * Cria um transformador de filtro
   * @param {Object|Function} filter - Critérios de filtro ou função de filtro
   * @param {Object} options - Opções adicionais
   * @returns {FilterTransformer} - Transformador de filtro
   */
  static createFilter(filter, options = {}) {
    return new FilterTransformer({
      ...options,
      filter
    });
  }

  /**
   * Cria um transformador de mapeamento
   * @param {Function} mapper - Função de mapeamento
   * @param {Object} options - Opções adicionais
   * @returns {MapTransformer} - Transformador de mapeamento
   */
  static createMapper(mapper, options = {}) {
    return createMapper(mapper, options);
  }

  /**
   * Cria um transformador que seleciona campos específicos
   * @param {string[]} fields - Lista de campos a serem mantidos
   * @param {Object} options - Opções adicionais
   * @returns {MapTransformer} - Transformador de seleção de campos
   */
  static createFieldSelector(fields, options = {}) {
    return createFieldSelector(fields, options);
  }

  /**
   * Cria um transformador de agregação
   * @param {string|Function} keyFn - Campo ou função para extrair a chave
   * @param {Function} aggregateFn - Função de agregação
   * @param {Object} options - Opções adicionais
   * @returns {AggregateTransformer} - Transformador de agregação
   */
  static createAggregator(keyFn, aggregateFn, options = {}) {
    return new AggregateTransformer({
      ...options,
      keyFn,
      aggregateFn
    });
  }

  /**
   * Cria um transformador de contagem
   * @param {string|Function} keyField - Campo ou função para extrair a chave
   * @param {Object} options - Opções adicionais
   * @returns {AggregateTransformer} - Transformador de contagem
   */
  static createCounter(keyField, options = {}) {
    return createCounter(keyField, options);
  }

  /**
   * Cria um transformador de estatísticas
   * @param {string|Function} keyField - Campo ou função para extrair a chave
   * @param {string} valueField - Campo contendo o valor numérico
   * @param {Object} options - Opções adicionais
   * @returns {AggregateTransformer} - Transformador de estatísticas
   */
  static createStats(keyField, valueField, options = {}) {
    return createStats(keyField, valueField, options);
  }

  /**
   * Cria um transformador que adiciona campos calculados
   * @param {Object} fields - Objeto com funções para calcular campos
   * @param {Object} options - Opções adicionais
   * @returns {MapTransformer} - Transformador de adição de campos
   */
  static createFieldAdder(fields, options = {}) {
    return createMapper((record) => {
      const result = { ...record };
      
      for (const [fieldName, fieldFn] of Object.entries(fields)) {
        try {
          result[fieldName] = fieldFn(record);
        } catch (error) {
          logger.warn({
            field: fieldName,
            error: error.message,
            record
          }, 'Error calculating field');
        }
      }
      
      return result;
    }, {
      name: 'FieldAdder',
      ...options
    });
  }

  /**
   * Cria um transformador que normaliza campos
   * @param {Object} fieldMappings - Mapeamento de campos antigos para novos
   * @param {Object} options - Opções adicionais
   * @returns {MapTransformer} - Transformador de normalização
   */
  static createNormalizer(fieldMappings, options = {}) {
    return createMapper((record) => {
      const result = { ...record };
      
      for (const [oldField, newField] of Object.entries(fieldMappings)) {
        if (record[oldField] !== undefined) {
          result[newField] = record[oldField];
          
          // Remove o campo antigo se não for o mesmo que o novo
          if (oldField !== newField && options.removeOriginal !== false) {
            delete result[oldField];
          }
        }
      }
      
      return result;
    }, {
      name: 'Normalizer',
      ...options
    });
  }

  /**
   * Cria uma pipeline de transformadores
   * @param {Array} transformers - Array de transformadores
   * @returns {Array} - Array de transformadores
   */
  static createPipeline(transformers) {
    logger.debug({
      transformerCount: transformers.length,
      transformerTypes: transformers.map(t => t.constructor.name)
    }, 'Creating transformer pipeline');
    
    return transformers;
  }
}

module.exports = TransformerFactory;
