const { performance, PerformanceObserver } = require('perf_hooks');
const fs = require('fs');
const path = require('path');
const os = require('os');
const v8 = require('v8');
const logger = require('./logger');

/**
 * Classe para análise de performance (profiling)
 */
class Profiler {
  /**
   * Cria uma nova instância do profiler
   * @param {Object} options - Opções de configuração
   * @param {boolean} options.enabled - Se o profiling está habilitado
   * @param {string} options.outputDir - Diretório para salvar relatórios de profiling
   * @param {number} options.interval - Intervalo em ms para coletar métricas (padrão: 5000)
   */
  constructor(options = {}) {
    this.enabled = options.enabled !== false;
    this.outputDir = options.outputDir || path.join(process.cwd(), 'profiling');
    this.interval = options.interval || 5000;
    
    // Armazena as métricas coletadas
    this.metrics = {
      cpu: [],
      memory: [],
      gc: [],
      events: [],
      marks: {}
    };
    
    // Intervalos de coleta
    this.cpuInterval = null;
    this.memoryInterval = null;
    
    // Observer para eventos de garbage collection
    this.observer = null;
    
    // Marca o início da execução
    this.startTime = null;
    this.startMemory = null;
    
    logger.info({
      enabled: this.enabled,
      outputDir: this.outputDir,
      interval: this.interval
    }, 'Profiler initialized');
  }

  /**
   * Inicia o profiling
   */
  start() {
    if (!this.enabled) {
      return;
    }
    
    // Cria o diretório de saída se não existir
    if (!fs.existsSync(this.outputDir)) {
      fs.mkdirSync(this.outputDir, { recursive: true });
    }
    
    // Marca o início da execução
    this.startTime = performance.now();
    this.startMemory = process.memoryUsage();
    
    // Registra evento de início
    this.addEvent('profiler_start');
    
    // Configura o observer para eventos de garbage collection
    this.setupGCObserver();
    
    // Inicia a coleta de métricas de CPU
    this.cpuInterval = setInterval(() => this.collectCPUMetrics(), this.interval);
    
    // Inicia a coleta de métricas de memória
    this.memoryInterval = setInterval(() => this.collectMemoryMetrics(), this.interval);
    
    logger.info('Profiler started');
  }

  /**
   * Para o profiling e gera relatório
   */
  stop() {
    if (!this.enabled) {
      return;
    }
    
    // Registra evento de fim
    this.addEvent('profiler_stop');
    
    // Para a coleta de métricas
    if (this.cpuInterval) {
      clearInterval(this.cpuInterval);
      this.cpuInterval = null;
    }
    
    if (this.memoryInterval) {
      clearInterval(this.memoryInterval);
      this.memoryInterval = null;
    }
    
    // Desconecta o observer de GC
    if (this.observer) {
      this.observer.disconnect();
      this.observer = null;
    }
    
    // Gera relatório
    this.generateReport();
    
    logger.info('Profiler stopped');
  }

  /**
   * Configura o observer para eventos de garbage collection
   */
  setupGCObserver() {
    // Cria um observer para eventos de performance
    this.observer = new PerformanceObserver((list) => {
      const entries = list.getEntries();
      
      // Processa cada entrada
      entries.forEach(entry => {
        if (entry.entryType === 'gc') {
          this.metrics.gc.push({
            timestamp: performance.now(),
            duration: entry.duration,
            kind: entry.kind,
            flags: entry.flags
          });
        }
      });
    });
    
    // Observa eventos de garbage collection
    this.observer.observe({ entryTypes: ['gc'], buffered: true });
  }

  /**
   * Coleta métricas de CPU
   */
  collectCPUMetrics() {
    const cpuUsage = process.cpuUsage();
    const timestamp = performance.now();
    
    this.metrics.cpu.push({
      timestamp,
      user: cpuUsage.user,
      system: cpuUsage.system
    });
  }

  /**
   * Coleta métricas de memória
   */
  collectMemoryMetrics() {
    const memoryUsage = process.memoryUsage();
    const timestamp = performance.now();
    
    this.metrics.memory.push({
      timestamp,
      rss: memoryUsage.rss,
      heapTotal: memoryUsage.heapTotal,
      heapUsed: memoryUsage.heapUsed,
      external: memoryUsage.external,
      arrayBuffers: memoryUsage.arrayBuffers
    });
  }

  /**
   * Adiciona um evento ao timeline
   * @param {string} name - Nome do evento
   * @param {Object} data - Dados adicionais do evento
   */
  addEvent(name, data = {}) {
    if (!this.enabled) {
      return;
    }
    
    this.metrics.events.push({
      name,
      timestamp: performance.now(),
      data
    });
  }

  /**
   * Marca o início de uma operação
   * @param {string} name - Nome da operação
   */
  mark(name) {
    if (!this.enabled) {
      return;
    }
    
    if (!this.metrics.marks[name]) {
      this.metrics.marks[name] = [];
    }
    
    this.metrics.marks[name].push({
      start: performance.now(),
      end: null,
      duration: null
    });
    
    logger.debug(`Mark start: ${name}`);
  }

  /**
   * Marca o fim de uma operação
   * @param {string} name - Nome da operação
   */
  markEnd(name) {
    if (!this.enabled || !this.metrics.marks[name] || this.metrics.marks[name].length === 0) {
      return;
    }
    
    const mark = this.metrics.marks[name][this.metrics.marks[name].length - 1];
    mark.end = performance.now();
    mark.duration = mark.end - mark.start;
    
    logger.debug(`Mark end: ${name}, duration: ${mark.duration.toFixed(2)}ms`);
  }

  /**
   * Gera um relatório de profiling
   */
  generateReport() {
    if (!this.enabled) {
      return;
    }
    
    try {
      // Coleta informações do sistema
      const systemInfo = {
        platform: process.platform,
        arch: process.arch,
        nodeVersion: process.version,
        cpus: os.cpus().length,
        totalMemory: os.totalmem(),
        freeMemory: os.freemem()
      };
      
      // Calcula estatísticas de memória
      const memoryStats = this.calculateMemoryStats();
      
      // Calcula estatísticas de CPU
      const cpuStats = this.calculateCPUStats();
      
      // Calcula estatísticas de GC
      const gcStats = this.calculateGCStats();
      
      // Calcula estatísticas de marcas
      const markStats = this.calculateMarkStats();
      
      // Cria o relatório
      const report = {
        timestamp: new Date().toISOString(),
        duration: performance.now() - this.startTime,
        systemInfo,
        memoryStats,
        cpuStats,
        gcStats,
        markStats,
        events: this.metrics.events
      };
      
      // Salva o relatório em um arquivo
      const reportFile = path.join(this.outputDir, `profile_${Date.now()}.json`);
      fs.writeFileSync(reportFile, JSON.stringify(report, null, 2));
      
      // Salva um relatório resumido
      const summaryFile = path.join(this.outputDir, `profile_summary_${Date.now()}.json`);
      fs.writeFileSync(summaryFile, JSON.stringify({
        timestamp: report.timestamp,
        duration: report.duration,
        systemInfo,
        memory: {
          peak: memoryStats.peak,
          average: memoryStats.average
        },
        cpu: {
          average: cpuStats.average
        },
        gc: {
          count: gcStats.count,
          totalTime: gcStats.totalTime
        },
        marks: Object.entries(markStats).reduce((acc, [key, value]) => {
          acc[key] = {
            count: value.count,
            totalTime: value.totalTime,
            averageTime: value.averageTime
          };
          return acc;
        }, {})
      }, null, 2));
      
      logger.info({
        reportFile,
        summaryFile,
        duration: report.duration,
        peakMemory: memoryStats.peak.heapUsed
      }, 'Profiling report generated');
      
      return {
        reportFile,
        summaryFile
      };
    } catch (error) {
      logger.error({
        error: error.message
      }, 'Error generating profiling report');
    }
  }

  /**
   * Calcula estatísticas de memória
   * @returns {Object} - Estatísticas de memória
   */
  calculateMemoryStats() {
    if (this.metrics.memory.length === 0) {
      return {
        peak: this.startMemory,
        average: this.startMemory
      };
    }
    
    // Encontra o pico de uso de memória
    const peak = this.metrics.memory.reduce((max, current) => {
      return current.heapUsed > max.heapUsed ? current : max;
    }, this.metrics.memory[0]);
    
    // Calcula a média de uso de memória
    const average = {
      rss: this.metrics.memory.reduce((sum, m) => sum + m.rss, 0) / this.metrics.memory.length,
      heapTotal: this.metrics.memory.reduce((sum, m) => sum + m.heapTotal, 0) / this.metrics.memory.length,
      heapUsed: this.metrics.memory.reduce((sum, m) => sum + m.heapUsed, 0) / this.metrics.memory.length,
      external: this.metrics.memory.reduce((sum, m) => sum + m.external, 0) / this.metrics.memory.length
    };
    
    return {
      peak,
      average
    };
  }

  /**
   * Calcula estatísticas de CPU
   * @returns {Object} - Estatísticas de CPU
   */
  calculateCPUStats() {
    if (this.metrics.cpu.length === 0) {
      return {
        average: { user: 0, system: 0 }
      };
    }
    
    // Calcula a média de uso de CPU
    const average = {
      user: this.metrics.cpu.reduce((sum, c) => sum + c.user, 0) / this.metrics.cpu.length,
      system: this.metrics.cpu.reduce((sum, c) => sum + c.system, 0) / this.metrics.cpu.length
    };
    
    return {
      average
    };
  }

  /**
   * Calcula estatísticas de garbage collection
   * @returns {Object} - Estatísticas de GC
   */
  calculateGCStats() {
    if (this.metrics.gc.length === 0) {
      return {
        count: 0,
        totalTime: 0,
        averageTime: 0
      };
    }
    
    const count = this.metrics.gc.length;
    const totalTime = this.metrics.gc.reduce((sum, gc) => sum + gc.duration, 0);
    const averageTime = totalTime / count;
    
    return {
      count,
      totalTime,
      averageTime
    };
  }

  /**
   * Calcula estatísticas de marcas
   * @returns {Object} - Estatísticas de marcas
   */
  calculateMarkStats() {
    const stats = {};
    
    for (const [name, marks] of Object.entries(this.metrics.marks)) {
      const completedMarks = marks.filter(m => m.duration !== null);
      
      if (completedMarks.length === 0) {
        stats[name] = {
          count: 0,
          totalTime: 0,
          averageTime: 0,
          minTime: 0,
          maxTime: 0
        };
        continue;
      }
      
      const count = completedMarks.length;
      const totalTime = completedMarks.reduce((sum, m) => sum + m.duration, 0);
      const averageTime = totalTime / count;
      const minTime = Math.min(...completedMarks.map(m => m.duration));
      const maxTime = Math.max(...completedMarks.map(m => m.duration));
      
      stats[name] = {
        count,
        totalTime,
        averageTime,
        minTime,
        maxTime
      };
    }
    
    return stats;
  }

  /**
   * Cria uma instância de profiler
   * @param {Object} options - Opções de configuração
   * @returns {Profiler} - Instância do profiler
   */
  static create(options = {}) {
    return new Profiler(options);
  }
}

module.exports = Profiler;
