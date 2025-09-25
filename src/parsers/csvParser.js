const { Transform } = require('stream');
const csvParser = require('csv-parser');
const { Readable } = require('stream');
const logger = require('../utils/logger');

/**
 * Parser para formato CSV
 * Converte cada linha do stream em um objeto JavaScript
 */
class CSVParser extends Transform {
  /**
   * Cria uma nova instância do parser CSV
   * @param {Object} options - Opções do parser CSV
   */
  constructor(options = {}) {
    // Configuramos o modo objectMode para emitir objetos em vez de strings/buffers
    super({ objectMode: true });
    
    // Opções para o parser CSV
    this.parserOptions = {
      separator: options.separator || ',',
      quote: options.quote || '"',
      escape: options.escape || '"',
      header: options.header !== false, // Por padrão, assume que a primeira linha é o cabeçalho
      skipLines: options.skipLines || 0,
      ...options
    };
    
    // Cria o parser CSV interno
    this.csvParserStream = csvParser(this.parserOptions);
    
    // Contador de linhas processadas
    this.lineCount = 0;
    
    // Configura eventos para o parser CSV
    this.csvParserStream.on('data', (data) => {
      this.lineCount++;
      this.push(data);
    });
    
    this.csvParserStream.on('error', (error) => {
      logger.error({ error: error.message }, 'Error parsing CSV');
      this.emit('error', error);
    });
  }

  /**
   * Processa um chunk de dados
   * @param {Buffer|string} chunk - Chunk de dados a ser processado
   * @param {string} encoding - Codificação do chunk
   * @param {function} callback - Função de callback
   */
  _transform(chunk, encoding, callback) {
    try {
      // Passa o chunk para o parser CSV interno
      this.csvParserStream.write(chunk, encoding);
      callback();
    } catch (error) {
      logger.error({ error: error.message }, 'Error in CSV parser transform');
      callback(error);
    }
  }

  /**
   * Processa dados restantes quando o stream de entrada termina
   * @param {function} callback - Função de callback
   */
  _flush(callback) {
    try {
      // Finaliza o parser CSV interno
      this.csvParserStream.end();
      
      logger.info(`CSV parsing completed: ${this.lineCount} lines processed`);
      callback();
    } catch (error) {
      logger.error({ error: error.message }, 'Error in CSV parser flush');
      callback(error);
    }
  }
}

/**
 * Função auxiliar para criar um parser CSV com opções específicas
 * @param {Object} options - Opções do parser CSV
 * @returns {CSVParser} - Instância do parser CSV
 */
function createCSVParser(options = {}) {
  return new CSVParser(options);
}

module.exports = {
  CSVParser,
  createCSVParser
};
