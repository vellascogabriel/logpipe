const NDJSONParser = require('./ndjsonParser');
const { createCSVParser } = require('./csvParser');
const logger = require('../utils/logger');
const path = require('path');

/**
 * Cria um parser apropriado com base no formato especificado
 * @param {string} format - Formato do arquivo (ndjson, csv)
 * @param {Object} options - Opções específicas do parser
 * @returns {stream.Transform} - Stream de transformação para parsing
 */
function createParser(format, options = {}) {
  // Normaliza o formato para minúsculas
  const normalizedFormat = format.toLowerCase();
  
  logger.debug(`Creating parser for format: ${normalizedFormat}`);
  
  switch (normalizedFormat) {
    case 'ndjson':
    case 'json':
      logger.debug('Using NDJSON parser');
      return new NDJSONParser(options);
      
    case 'csv':
      logger.debug('Using CSV parser');
      return createCSVParser(options);
      
    default:
      throw new Error(`Formato não suportado: ${format}. Formatos suportados: ndjson, csv`);
  }
}

/**
 * Detecta o formato do arquivo com base na extensão
 * @param {string} filePath - Caminho do arquivo
 * @returns {string} - Formato detectado (ndjson, csv)
 */
function detectFormatFromFilename(filePath) {
  const extension = path.extname(filePath).toLowerCase();
  
  // Remove o ponto inicial da extensão
  const ext = extension.startsWith('.') ? extension.substring(1) : extension;
  
  // Mapeia extensões para formatos
  switch (ext) {
    case 'json':
    case 'ndjson':
    case 'jsonl':
      return 'ndjson';
      
    case 'csv':
      return 'csv';
      
    // Se o arquivo for compactado, verifica a extensão antes da compressão
    case 'gz':
    case 'gzip':
      const baseFilename = path.basename(filePath, extension);
      const baseExtension = path.extname(baseFilename).toLowerCase();
      
      if (baseExtension === '.json' || baseExtension === '.ndjson' || baseExtension === '.jsonl') {
        return 'ndjson';
      } else if (baseExtension === '.csv') {
        return 'csv';
      }
      break;
  }
  
  // Formato não detectado
  return null;
}

/**
 * Cria um parser apropriado com base no caminho do arquivo e no formato especificado
 * @param {string} filePath - Caminho do arquivo
 * @param {string} [specifiedFormat] - Formato especificado pelo usuário (opcional)
 * @param {Object} options - Opções específicas do parser
 * @returns {stream.Transform} - Stream de transformação para parsing
 */
function createParserForFile(filePath, specifiedFormat, options = {}) {
  // Se o formato foi especificado, usa-o
  if (specifiedFormat) {
    return createParser(specifiedFormat, options);
  }
  
  // Caso contrário, tenta detectar o formato pelo nome do arquivo
  const detectedFormat = detectFormatFromFilename(filePath);
  
  if (detectedFormat) {
    logger.info(`Formato detectado automaticamente: ${detectedFormat}`);
    return createParser(detectedFormat, options);
  }
  
  // Se não conseguiu detectar, usa NDJSON como padrão
  logger.warn(`Não foi possível detectar o formato do arquivo ${filePath}. Usando NDJSON como padrão.`);
  return createParser('ndjson', options);
}

module.exports = {
  createParser,
  detectFormatFromFilename,
  createParserForFile
};
