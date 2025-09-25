const BaseTransformer = require('./baseTransformer');
const logger = require('../utils/logger');

/**
 * Transformador que filtra registros com base em critérios específicos
 */
class FilterTransformer extends BaseTransformer {
  /**
   * Cria uma nova instância do transformador de filtro
   * @param {Object} options - Opções do transformador
   * @param {Function|Object} options.filter - Função de filtro ou objeto com critérios
   */
  constructor(options = {}) {
    super(options);
    
    if (!options.filter) {
      throw new Error('A opção filter é obrigatória para o FilterTransformer');
    }
    
    // Se filter for uma função, usa-a diretamente
    if (typeof options.filter === 'function') {
      this.filterFn = options.filter;
    } 
    // Se filter for um objeto, cria uma função que verifica se o registro corresponde aos critérios
    else if (typeof options.filter === 'object') {
      this.filterFn = this.createFilterFromObject(options.filter);
    } 
    else {
      throw new Error('A opção filter deve ser uma função ou um objeto');
    }
    
    // Modo de inclusão ou exclusão
    this.includeMatches = options.includeMatches !== false; // padrão: true (incluir correspondências)
    
    logger.debug({
      transformer: this.name,
      includeMatches: this.includeMatches,
      filterCriteria: typeof options.filter === 'object' ? options.filter : 'custom function'
    }, 'Filter transformer initialized');
  }

  /**
   * Cria uma função de filtro a partir de um objeto de critérios
   * @param {Object} criteria - Objeto com critérios de filtro
   * @returns {Function} - Função de filtro
   */
  createFilterFromObject(criteria) {
    return (record) => {
      // Verifica cada critério no objeto
      for (const [key, value] of Object.entries(criteria)) {
        // Suporta caminhos aninhados com notação de ponto (ex: "user.name")
        const recordValue = this.getNestedValue(record, key);
        
        // Se o valor for uma regex, testa contra o valor do registro
        if (value instanceof RegExp) {
          if (!value.test(String(recordValue))) {
            return false;
          }
        }
        // Se o valor for uma função, chama-a com o valor do registro
        else if (typeof value === 'function') {
          if (!value(recordValue)) {
            return false;
          }
        }
        // Caso contrário, compara diretamente
        else if (recordValue !== value) {
          return false;
        }
      }
      
      // Se passou por todos os critérios, o registro corresponde
      return true;
    };
  }

  /**
   * Obtém um valor aninhado de um objeto usando notação de ponto
   * @param {Object} obj - Objeto a ser consultado
   * @param {string} path - Caminho para o valor (ex: "user.name")
   * @returns {*} - Valor encontrado ou undefined
   */
  getNestedValue(obj, path) {
    return path.split('.').reduce((o, key) => (o && o[key] !== undefined) ? o[key] : undefined, obj);
  }

  /**
   * Transforma (filtra) um registro
   * @param {Object} record - Registro a ser filtrado
   * @param {function} callback - Função de callback
   */
  transformRecord(record, callback) {
    try {
      // Aplica a função de filtro
      const matches = this.filterFn(record);
      
      // Decide se deve incluir ou excluir com base no modo e no resultado do filtro
      const shouldInclude = this.includeMatches ? matches : !matches;
      
      // Se deve incluir, passa o registro; caso contrário, passa null para filtrar
      callback(null, shouldInclude ? record : null);
    } catch (error) {
      logger.error({
        transformer: this.name,
        error: error.message,
        record
      }, 'Error applying filter');
      callback(error);
    }
  }
}

module.exports = FilterTransformer;
