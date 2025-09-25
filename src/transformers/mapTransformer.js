const BaseTransformer = require('./baseTransformer');
const logger = require('../utils/logger');

/**
 * Transformador que mapeia/modifica registros
 */
class MapTransformer extends BaseTransformer {
  /**
   * Cria uma nova instância do transformador de mapeamento
   * @param {Object} options - Opções do transformador
   * @param {Function} options.mapper - Função de mapeamento que recebe um registro e retorna um novo registro
   */
  constructor(options = {}) {
    super(options);
    
    if (!options.mapper || typeof options.mapper !== 'function') {
      throw new Error('A opção mapper deve ser uma função');
    }
    
    this.mapperFn = options.mapper;
    
    // Opção para preservar o registro original em caso de erro
    this.preserveOnError = options.preserveOnError !== false; // padrão: true
    
    logger.debug({
      transformer: this.name,
      preserveOnError: this.preserveOnError
    }, 'Map transformer initialized');
  }

  /**
   * Transforma um registro aplicando a função de mapeamento
   * @param {Object} record - Registro a ser transformado
   * @param {function} callback - Função de callback
   */
  transformRecord(record, callback) {
    try {
      // Aplica a função de mapeamento
      const mappedRecord = this.mapperFn(record);
      
      // Passa o registro mapeado para o próximo transformador
      callback(null, mappedRecord);
    } catch (error) {
      logger.error({
        transformer: this.name,
        error: error.message,
        record
      }, 'Error mapping record');
      
      // Se preserveOnError for true, passa o registro original em caso de erro
      if (this.preserveOnError) {
        logger.debug('Preserving original record due to mapping error');
        callback(null, record);
      } else {
        // Caso contrário, propaga o erro
        callback(error);
      }
    }
  }
}

/**
 * Função auxiliar para criar um transformador de mapeamento com uma função simples
 * @param {Function} mapperFn - Função de mapeamento
 * @param {Object} options - Opções adicionais
 * @returns {MapTransformer} - Instância do transformador
 */
function createMapper(mapperFn, options = {}) {
  return new MapTransformer({
    ...options,
    mapper: mapperFn
  });
}

/**
 * Função auxiliar para criar um transformador que seleciona campos específicos
 * @param {string[]} fields - Lista de campos a serem mantidos
 * @param {Object} options - Opções adicionais
 * @returns {MapTransformer} - Instância do transformador
 */
function createFieldSelector(fields, options = {}) {
  return new MapTransformer({
    ...options,
    name: 'FieldSelector',
    mapper: (record) => {
      const result = {};
      for (const field of fields) {
        if (field.includes('.')) {
          // Suporta caminhos aninhados
          const parts = field.split('.');
          let current = record;
          let target = result;
          
          for (let i = 0; i < parts.length; i++) {
            const part = parts[i];
            if (current && current[part] !== undefined) {
              if (i === parts.length - 1) {
                // Último nível, copia o valor
                target[part] = current[part];
              } else {
                // Nível intermediário, cria objeto se necessário
                target[part] = target[part] || {};
                target = target[part];
                current = current[part];
              }
            } else {
              break;
            }
          }
        } else if (record[field] !== undefined) {
          // Campo simples
          result[field] = record[field];
        }
      }
      return result;
    }
  });
}

module.exports = {
  MapTransformer,
  createMapper,
  createFieldSelector
};
