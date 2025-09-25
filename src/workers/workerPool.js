const { Worker } = require('worker_threads');
const os = require('os');
const path = require('path');
const { EventEmitter } = require('events');
const logger = require('../utils/logger');

/**
 * Pool de worker threads para processamento paralelo
 * Gerencia a criação, distribuição de tarefas e ciclo de vida dos workers
 */
class WorkerPool extends EventEmitter {
  /**
   * Cria uma nova instância do pool de workers
   * @param {Object} options - Opções de configuração
   * @param {number} options.numWorkers - Número de workers a serem criados (padrão: número de CPUs)
   * @param {string} options.workerScript - Caminho para o script do worker
   * @param {Object} options.workerData - Dados a serem passados para os workers na inicialização
   */
  constructor(options = {}) {
    super();
    
    this.numWorkers = options.numWorkers || os.cpus().length;
    this.workerScript = options.workerScript || path.join(__dirname, 'worker.js');
    this.workerData = options.workerData || {};
    
    // Armazena os workers ativos
    this.workers = [];
    
    // Fila de tarefas pendentes
    this.taskQueue = [];
    
    // Rastreia workers ocupados
    this.busyWorkers = new Set();
    
    // Estatísticas
    this.stats = {
      tasksSubmitted: 0,
      tasksCompleted: 0,
      tasksErrored: 0,
      totalProcessingTime: 0
    };
    
    // Flag para indicar se o pool está sendo encerrado
    this.shuttingDown = false;
    
    logger.info({
      numWorkers: this.numWorkers,
      workerScript: this.workerScript
    }, 'Initializing worker pool');
  }

  /**
   * Inicializa o pool de workers
   * @returns {Promise<void>} - Promise resolvida quando todos os workers estiverem prontos
   */
  async init() {
    logger.debug('Starting worker initialization');
    
    const initPromises = [];
    
    for (let i = 0; i < this.numWorkers; i++) {
      initPromises.push(this.createWorker(i));
    }
    
    await Promise.all(initPromises);
    
    logger.info(`Worker pool initialized with ${this.workers.length} workers`);
    this.emit('ready');
  }

  /**
   * Cria um novo worker
   * @param {number} id - ID do worker
   * @returns {Promise<Worker>} - Promise resolvida quando o worker estiver pronto
   */
  createWorker(id) {
    return new Promise((resolve, reject) => {
      logger.debug(`Creating worker ${id}`);
      
      const worker = new Worker(this.workerScript, {
        workerData: {
          workerId: id,
          ...this.workerData
        }
      });
      
      // Configura manipuladores de eventos
      worker.on('message', (message) => this.handleWorkerMessage(worker, message));
      
      worker.on('error', (error) => {
        logger.error({
          workerId: id,
          error: error.message
        }, 'Worker error');
        this.emit('worker:error', { worker, error });
        
        // Recria o worker se o pool não estiver sendo encerrado
        if (!this.shuttingDown) {
          this.replaceWorker(worker);
        }
      });
      
      worker.on('exit', (code) => {
        logger.debug({
          workerId: id,
          exitCode: code
        }, 'Worker exited');
        
        // Recria o worker se saiu com erro e o pool não estiver sendo encerrado
        if (code !== 0 && !this.shuttingDown) {
          logger.warn(`Worker ${id} exited with code ${code}, replacing...`);
          this.replaceWorker(worker);
        }
      });
      
      // Armazena o worker no array
      worker.id = id;
      this.workers.push(worker);
      
      // Aguarda mensagem de pronto do worker
      worker.once('message', (message) => {
        if (message.type === 'ready') {
          logger.debug(`Worker ${id} is ready`);
          resolve(worker);
        } else {
          reject(new Error(`Unexpected message from worker ${id}: ${JSON.stringify(message)}`));
        }
      });
    });
  }

  /**
   * Substitui um worker que falhou
   * @param {Worker} oldWorker - Worker a ser substituído
   */
  async replaceWorker(oldWorker) {
    const id = oldWorker.id;
    
    // Remove o worker antigo do array
    const index = this.workers.indexOf(oldWorker);
    if (index !== -1) {
      this.workers.splice(index, 1);
    }
    
    // Remove da lista de ocupados
    this.busyWorkers.delete(oldWorker);
    
    try {
      // Cria um novo worker com o mesmo ID
      await this.createWorker(id);
      logger.info(`Worker ${id} successfully replaced`);
      
      // Processa a próxima tarefa na fila, se houver
      this.processQueue();
    } catch (error) {
      logger.error({
        workerId: id,
        error: error.message
      }, 'Failed to replace worker');
    }
  }

  /**
   * Manipula mensagens recebidas dos workers
   * @param {Worker} worker - Worker que enviou a mensagem
   * @param {Object} message - Mensagem recebida
   */
  handleWorkerMessage(worker, message) {
    switch (message.type) {
      case 'result':
        this.handleTaskResult(worker, message);
        break;
        
      case 'error':
        this.handleTaskError(worker, message);
        break;
        
      case 'stats':
        this.handleWorkerStats(worker, message);
        break;
        
      default:
        logger.debug({
          workerId: worker.id,
          message
        }, 'Received unknown message type from worker');
    }
  }

  /**
   * Manipula o resultado de uma tarefa concluída
   * @param {Worker} worker - Worker que concluiu a tarefa
   * @param {Object} message - Mensagem com o resultado
   */
  handleTaskResult(worker, message) {
    const { taskId, result, processingTime } = message;
    
    logger.debug({
      workerId: worker.id,
      taskId,
      processingTime
    }, 'Task completed by worker');
    
    // Atualiza estatísticas
    this.stats.tasksCompleted++;
    this.stats.totalProcessingTime += processingTime;
    
    // Marca o worker como disponível
    this.busyWorkers.delete(worker);
    
    // Emite evento de conclusão
    this.emit('task:completed', { taskId, result, worker, processingTime });
    
    // Processa a próxima tarefa na fila
    this.processQueue();
  }

  /**
   * Manipula erro em uma tarefa
   * @param {Worker} worker - Worker que encontrou o erro
   * @param {Object} message - Mensagem com o erro
   */
  handleTaskError(worker, message) {
    const { taskId, error } = message;
    
    logger.error({
      workerId: worker.id,
      taskId,
      error
    }, 'Task error in worker');
    
    // Atualiza estatísticas
    this.stats.tasksErrored++;
    
    // Marca o worker como disponível
    this.busyWorkers.delete(worker);
    
    // Emite evento de erro
    this.emit('task:error', { taskId, error, worker });
    
    // Processa a próxima tarefa na fila
    this.processQueue();
  }

  /**
   * Manipula estatísticas enviadas por um worker
   * @param {Worker} worker - Worker que enviou as estatísticas
   * @param {Object} message - Mensagem com as estatísticas
   */
  handleWorkerStats(worker, message) {
    logger.debug({
      workerId: worker.id,
      stats: message.stats
    }, 'Received worker stats');
    
    // Emite evento de estatísticas
    this.emit('worker:stats', { worker, stats: message.stats });
  }

  /**
   * Submete uma tarefa para processamento
   * @param {Object} task - Tarefa a ser processada
   * @param {string} task.type - Tipo da tarefa
   * @param {*} task.data - Dados da tarefa
   * @returns {Promise<*>} - Promise resolvida com o resultado da tarefa
   */
  submitTask(task) {
    return new Promise((resolve, reject) => {
      // Gera um ID único para a tarefa
      const taskId = `task_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
      
      // Cria um objeto de tarefa completo
      const taskObj = {
        id: taskId,
        ...task,
        timestamp: Date.now()
      };
      
      // Configura callbacks para esta tarefa específica
      const onCompleted = ({ taskId: completedTaskId, result }) => {
        if (completedTaskId === taskId) {
          this.removeListeners(taskId);
          resolve(result);
        }
      };
      
      const onError = ({ taskId: errorTaskId, error }) => {
        if (errorTaskId === taskId) {
          this.removeListeners(taskId);
          reject(new Error(error));
        }
      };
      
      // Registra os listeners
      this.on('task:completed', onCompleted);
      this.on('task:error', onError);
      
      // Armazena os listeners para remoção posterior
      taskObj.listeners = { onCompleted, onError };
      
      // Adiciona a tarefa à fila
      this.taskQueue.push(taskObj);
      this.stats.tasksSubmitted++;
      
      logger.debug({
        taskId,
        taskType: task.type,
        queueLength: this.taskQueue.length
      }, 'Task submitted to worker pool');
      
      // Tenta processar a fila imediatamente
      this.processQueue();
    });
  }

  /**
   * Remove os listeners de uma tarefa
   * @param {string} taskId - ID da tarefa
   */
  removeListeners(taskId) {
    // Encontra a tarefa na fila
    const taskIndex = this.taskQueue.findIndex(t => t.id === taskId);
    if (taskIndex !== -1) {
      const task = this.taskQueue[taskIndex];
      if (task.listeners) {
        this.off('task:completed', task.listeners.onCompleted);
        this.off('task:error', task.listeners.onError);
      }
    }
  }

  /**
   * Processa a fila de tarefas
   */
  processQueue() {
    // Se não há tarefas na fila, não faz nada
    if (this.taskQueue.length === 0) {
      return;
    }
    
    // Encontra workers disponíveis
    const availableWorkers = this.workers.filter(w => !this.busyWorkers.has(w));
    
    if (availableWorkers.length === 0) {
      logger.debug('No available workers, tasks will wait in queue');
      return;
    }
    
    // Distribui tarefas para workers disponíveis
    while (this.taskQueue.length > 0 && availableWorkers.length > 0) {
      const worker = availableWorkers.pop();
      const task = this.taskQueue.shift();
      
      logger.debug({
        workerId: worker.id,
        taskId: task.id,
        taskType: task.type
      }, 'Assigning task to worker');
      
      // Marca o worker como ocupado
      this.busyWorkers.add(worker);
      
      // Envia a tarefa para o worker
      worker.postMessage({
        type: 'task',
        taskId: task.id,
        taskType: task.type,
        data: task.data
      });
    }
  }

  /**
   * Obtém estatísticas do pool de workers
   * @returns {Object} - Estatísticas do pool
   */
  getStats() {
    return {
      ...this.stats,
      workersTotal: this.workers.length,
      workersBusy: this.busyWorkers.size,
      workersAvailable: this.workers.length - this.busyWorkers.size,
      queueLength: this.taskQueue.length,
      avgProcessingTime: this.stats.tasksCompleted > 0 
        ? this.stats.totalProcessingTime / this.stats.tasksCompleted 
        : 0
    };
  }

  /**
   * Encerra o pool de workers
   * @param {boolean} force - Se true, encerra imediatamente sem esperar tarefas em andamento
   * @returns {Promise<void>} - Promise resolvida quando todos os workers forem encerrados
   */
  async shutdown(force = false) {
    logger.info('Shutting down worker pool');
    this.shuttingDown = true;
    
    if (!force && this.taskQueue.length > 0) {
      logger.info(`Waiting for ${this.taskQueue.length} pending tasks to complete`);
      
      // Espera todas as tarefas serem concluídas
      await new Promise(resolve => {
        const checkQueue = () => {
          if (this.taskQueue.length === 0 && this.busyWorkers.size === 0) {
            resolve();
          } else {
            setTimeout(checkQueue, 100);
          }
        };
        checkQueue();
      });
    }
    
    // Encerra todos os workers
    const terminationPromises = this.workers.map(worker => {
      return new Promise((resolve) => {
        worker.once('exit', resolve);
        worker.terminate();
      });
    });
    
    await Promise.all(terminationPromises);
    logger.info('All workers terminated');
    
    this.workers = [];
    this.busyWorkers.clear();
    this.emit('shutdown');
  }
}

module.exports = WorkerPool;
