const { Transform } = require('stream');
const logger = require('../utils/logger');

/**
 * Parser para formato NDJSON (Newline Delimited JSON)
 * Converte cada linha do stream em um objeto JavaScript
 */
class NDJSONParser extends Transform {
  /**
   * Cria uma nova instância do parser NDJSON
   * @param {Object} options - Opções do stream Transform
   */
  constructor(options = {}) {
    // Configuramos o modo objectMode para emitir objetos em vez de strings/buffers
    super({ ...options, objectMode: true });
    
    // Buffer para armazenar dados incompletos entre chunks
    this.buffer = '';
    
    // Contador de linhas para rastreamento de erros
    this.lineCount = 0;
    
    // Contador de erros para limitar mensagens de log
    this.errorCount = 0;
    this.maxErrorLogs = options.maxErrorLogs || 10;
  }

  /**
   * Processa um chunk de dados
   * @param {Buffer|string} chunk - Chunk de dados a ser processado
   * @param {string} encoding - Codificação do chunk
   * @param {function} callback - Função de callback
   */
  _transform(chunk, encoding, callback) {
    try {
      // Converte o chunk para string e adiciona ao buffer
      const data = this.buffer + chunk.toString();
      
      // Divide o buffer em linhas
      const lines = data.split('\n');
      
      // A última linha pode estar incompleta, então a guardamos para o próximo chunk
      this.buffer = lines.pop();
      
      // Processa cada linha completa
      for (const line of lines) {
        this.lineCount++;
        
        // Ignora linhas vazias
        if (!line.trim()) continue;
        
        try {
          // Converte a linha JSON em objeto JavaScript
          const parsedObject = JSON.parse(line);
          
          // Emite o objeto para o próximo stream no pipeline
          this.push(parsedObject);
        } catch (parseError) {
          this.handleParseError(parseError, line);
        }
      }
      
      callback();
    } catch (error) {
      callback(error);
    }
  }

  /**
   * Processa dados restantes quando o stream de entrada termina
   * @param {function} callback - Função de callback
   */
  _flush(callback) {
    try {
      // Processa qualquer dado restante no buffer
      if (this.buffer.trim()) {
        this.lineCount++;
        
        try {
          const parsedObject = JSON.parse(this.buffer);
          this.push(parsedObject);
        } catch (parseError) {
          this.handleParseError(parseError, this.buffer);
        }
      }
      
      logger.info(`NDJSON parsing completed: ${this.lineCount} lines processed`);
      callback();
    } catch (error) {
      callback(error);
    }
  }

  /**
   * Trata erros de parsing
   * @param {Error} error - Erro de parsing
   * @param {string} line - Linha que causou o erro
   */
  handleParseError(error, line) {
    // Limita o número de erros logados para evitar inundar os logs
    if (this.errorCount < this.maxErrorLogs) {
      logger.warn({
        error: error.message,
        line: line.length > 100 ? `${line.substring(0, 100)}...` : line,
        lineNumber: this.lineCount
      }, 'Failed to parse NDJSON line');
      
      this.errorCount++;
      
      // Se atingiu o limite, loga uma mensagem informando
      if (this.errorCount === this.maxErrorLogs) {
        logger.warn(`Reached maximum error log limit (${this.maxErrorLogs}). Further parse errors will be counted but not logged.`);
      }
    }
  }
}

module.exports = NDJSONParser;
