const HttpSender = require('./httpSender');
const fs = require('fs');
const { Writable } = require('stream');
const logger = require('../utils/logger');

/**
 * Fábrica para criar diferentes tipos de senders
 */
class SenderFactory {
  /**
   * Cria um sender HTTP para enviar dados em lotes para um endpoint
   * @param {string} endpoint - URL do endpoint HTTP
   * @param {Object} options - Opções adicionais
   * @returns {HttpSender} - Instância do HttpSender
   */
  static createHttpSender(endpoint, options = {}) {
    return new HttpSender({
      endpoint,
      ...options
    });
  }

  /**
   * Cria um sender para arquivo
   * @param {string} filePath - Caminho do arquivo de saída
   * @param {Object} options - Opções adicionais
   * @returns {fs.WriteStream} - Stream de escrita para arquivo
   */
  static createFileSender(filePath, options = {}) {
    const fileStream = fs.createWriteStream(filePath, {
      flags: options.append ? 'a' : 'w',
      encoding: options.encoding || 'utf8'
    });
    
    logger.info({
      filePath,
      append: !!options.append
    }, 'File sender created');
    
    return fileStream;
  }

  /**
   * Cria um sender para console (stdout)
   * @param {Object} options - Opções adicionais
   * @returns {Writable} - Stream de escrita para console
   */
  static createConsoleSender(options = {}) {
    // Cria um stream de escrita que envia para stdout
    const consoleSender = new Writable({
      objectMode: true,
      write(chunk, encoding, callback) {
        try {
          // Formata o objeto como JSON
          const output = options.pretty
            ? JSON.stringify(chunk, null, 2)
            : JSON.stringify(chunk);
          
          process.stdout.write(output + '\n');
          callback();
        } catch (error) {
          callback(error);
        }
      }
    });
    
    logger.info({
      pretty: !!options.pretty
    }, 'Console sender created');
    
    return consoleSender;
  }

  /**
   * Cria um sender nulo (descarta os dados)
   * @returns {Writable} - Stream de escrita que descarta os dados
   */
  static createNullSender() {
    // Cria um stream de escrita que descarta os dados
    const nullSender = new Writable({
      objectMode: true,
      write(chunk, encoding, callback) {
        // Simplesmente ignora os dados
        callback();
      }
    });
    
    logger.info('Null sender created');
    
    return nullSender;
  }

  /**
   * Cria um sender com base no tipo especificado
   * @param {string} type - Tipo de sender (http, file, console, null)
   * @param {Object} options - Opções específicas do sender
   * @returns {Writable} - Stream de escrita
   */
  static createSender(type, options = {}) {
    switch (type.toLowerCase()) {
      case 'http':
        if (!options.endpoint) {
          throw new Error('Endpoint URL is required for HTTP sender');
        }
        return this.createHttpSender(options.endpoint, options);
        
      case 'file':
        if (!options.filePath) {
          throw new Error('File path is required for file sender');
        }
        return this.createFileSender(options.filePath, options);
        
      case 'console':
        return this.createConsoleSender(options);
        
      case 'null':
        return this.createNullSender();
        
      default:
        throw new Error(`Unknown sender type: ${type}`);
    }
  }
}

module.exports = SenderFactory;
