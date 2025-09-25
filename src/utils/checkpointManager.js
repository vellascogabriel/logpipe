const fs = require('fs');
const path = require('path');
const logger = require('./logger');

/**
 * Gerenciador de checkpoints para salvar e retomar o processamento
 */
class CheckpointManager {
  /**
   * Cria uma nova instância do gerenciador de checkpoints
   * @param {Object} options - Opções de configuração
   * @param {string} options.filePath - Caminho do arquivo de checkpoint
   * @param {number} options.interval - Intervalo em ms para salvar checkpoints (padrão: 30000)
   * @param {boolean} options.enabled - Se os checkpoints estão habilitados
   */
  constructor(options = {}) {
    this.filePath = options.filePath;
    this.interval = options.interval || 30000; // 30 segundos por padrão
    this.enabled = options.enabled !== false && !!this.filePath;
    
    // Estado atual do processamento
    this.state = {
      lastProcessedOffset: 0,
      lastProcessedLine: 0,
      recordsProcessed: 0,
      recordsFiltered: 0,
      recordsSent: 0,
      startTime: Date.now(),
      lastCheckpointTime: Date.now(),
      status: 'initialized'
    };
    
    // Carrega o checkpoint existente, se houver
    if (this.enabled) {
      this.loadCheckpoint();
    }
    
    // Intervalo para salvar checkpoints periodicamente
    this.checkpointInterval = null;
    
    logger.info({
      enabled: this.enabled,
      filePath: this.filePath,
      interval: this.interval
    }, 'Checkpoint manager initialized');
  }

  /**
   * Inicia o gerenciador de checkpoints
   */
  start() {
    if (!this.enabled) {
      return;
    }
    
    this.state.status = 'running';
    this.state.startTime = Date.now();
    
    // Configura o intervalo para salvar checkpoints periodicamente
    this.checkpointInterval = setInterval(() => {
      this.saveCheckpoint();
    }, this.interval);
    
    // Registra handlers para sinais do sistema
    this.registerSignalHandlers();
    
    logger.info('Checkpoint manager started');
  }

  /**
   * Para o gerenciador de checkpoints
   */
  stop() {
    if (!this.enabled) {
      return;
    }
    
    // Limpa o intervalo
    if (this.checkpointInterval) {
      clearInterval(this.checkpointInterval);
      this.checkpointInterval = null;
    }
    
    // Salva um checkpoint final
    this.state.status = 'completed';
    this.saveCheckpoint();
    
    logger.info('Checkpoint manager stopped');
  }

  /**
   * Registra handlers para sinais do sistema
   */
  registerSignalHandlers() {
    // Salva checkpoint ao receber sinais de interrupção
    const signalHandler = (signal) => {
      logger.info(`Received ${signal} signal, saving checkpoint...`);
      this.state.status = 'interrupted';
      this.saveCheckpoint();
      process.exit(0);
    };
    
    process.on('SIGINT', () => signalHandler('SIGINT'));
    process.on('SIGTERM', () => signalHandler('SIGTERM'));
  }

  /**
   * Atualiza o estado do processamento
   * @param {Object} update - Objeto com atualizações para o estado
   */
  updateState(update) {
    if (!this.enabled) {
      return;
    }
    
    Object.assign(this.state, update);
  }

  /**
   * Salva o checkpoint atual
   */
  saveCheckpoint() {
    if (!this.enabled) {
      return;
    }
    
    try {
      // Atualiza o timestamp do checkpoint
      this.state.lastCheckpointTime = Date.now();
      
      // Calcula estatísticas adicionais
      const elapsedTime = this.state.lastCheckpointTime - this.state.startTime;
      const stats = {
        ...this.state,
        elapsedTimeMs: elapsedTime,
        elapsedTimeFormatted: this.formatElapsedTime(elapsedTime),
        recordsPerSecond: elapsedTime > 0 ? (this.state.recordsProcessed / (elapsedTime / 1000)).toFixed(2) : 0
      };
      
      // Cria o diretório do checkpoint se não existir
      const checkpointDir = path.dirname(this.filePath);
      if (!fs.existsSync(checkpointDir)) {
        fs.mkdirSync(checkpointDir, { recursive: true });
      }
      
      // Salva o checkpoint em um arquivo temporário primeiro
      const tempFilePath = `${this.filePath}.tmp`;
      fs.writeFileSync(tempFilePath, JSON.stringify(stats, null, 2));
      
      // Renomeia o arquivo temporário para o arquivo final
      // Isso garante que o arquivo de checkpoint nunca fique em um estado inconsistente
      fs.renameSync(tempFilePath, this.filePath);
      
      logger.debug({
        checkpoint: this.filePath,
        recordsProcessed: stats.recordsProcessed,
        offset: stats.lastProcessedOffset,
        line: stats.lastProcessedLine
      }, 'Checkpoint saved');
    } catch (error) {
      logger.error({
        error: error.message,
        checkpoint: this.filePath
      }, 'Error saving checkpoint');
    }
  }

  /**
   * Carrega um checkpoint existente
   * @returns {boolean} - true se um checkpoint foi carregado, false caso contrário
   */
  loadCheckpoint() {
    if (!this.enabled || !fs.existsSync(this.filePath)) {
      return false;
    }
    
    try {
      const checkpointData = fs.readFileSync(this.filePath, 'utf8');
      const checkpoint = JSON.parse(checkpointData);
      
      // Verifica se o checkpoint é válido
      if (!checkpoint || typeof checkpoint !== 'object' || !checkpoint.lastProcessedOffset) {
        logger.warn({
          checkpoint: this.filePath
        }, 'Invalid checkpoint file');
        return false;
      }
      
      // Atualiza o estado com os dados do checkpoint
      Object.assign(this.state, checkpoint);
      
      // Atualiza o timestamp de início para calcular corretamente o tempo decorrido
      this.state.startTime = Date.now() - (checkpoint.elapsedTimeMs || 0);
      
      logger.info({
        checkpoint: this.filePath,
        recordsProcessed: checkpoint.recordsProcessed,
        offset: checkpoint.lastProcessedOffset,
        line: checkpoint.lastProcessedLine,
        status: checkpoint.status
      }, 'Checkpoint loaded');
      
      return true;
    } catch (error) {
      logger.error({
        error: error.message,
        checkpoint: this.filePath
      }, 'Error loading checkpoint');
      return false;
    }
  }

  /**
   * Verifica se o processamento deve ser retomado a partir de um checkpoint
   * @returns {boolean} - true se o processamento deve ser retomado, false caso contrário
   */
  shouldResume() {
    return this.enabled && 
           this.state.lastProcessedOffset > 0 && 
           (this.state.status === 'interrupted' || this.state.status === 'running');
  }

  /**
   * Obtém a posição de retomada
   * @returns {Object} - Objeto com offset e linha para retomar o processamento
   */
  getResumePosition() {
    return {
      offset: this.state.lastProcessedOffset,
      line: this.state.lastProcessedLine
    };
  }

  /**
   * Formata o tempo decorrido em formato legível
   * @param {number} ms - Tempo em milissegundos
   * @returns {string} - Tempo formatado
   */
  formatElapsedTime(ms) {
    const seconds = Math.floor(ms / 1000);
    const minutes = Math.floor(seconds / 60);
    const hours = Math.floor(minutes / 60);
    
    const remainingMinutes = minutes % 60;
    const remainingSeconds = seconds % 60;
    
    return `${hours}h ${remainingMinutes}m ${remainingSeconds}s`;
  }
}

module.exports = CheckpointManager;
