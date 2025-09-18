const { parentPort, workerData } = require('worker_threads');
const { performance } = require('perf_hooks');

// Identificador do worker
const workerId = workerData.workerId;

// Mapa de handlers de tarefas
const taskHandlers = {};

// Estatísticas do worker
const stats = {
  tasksProcessed: 0,
  tasksSucceeded: 0,
  tasksFailed: 0,
  totalProcessingTime: 0,
  startTime: Date.now()
};

// Intervalo para enviar estatísticas para o thread principal
const statsInterval = workerData.statsInterval || 30000; // 30 segundos por padrão

/**
 * Registra um handler para um tipo específico de tarefa
 * @param {string} taskType - Tipo da tarefa
 * @param {Function} handler - Função que processa a tarefa
 */
function registerTaskHandler(taskType, handler) {
  taskHandlers[taskType] = handler;
  console.log(`Worker ${workerId}: Registered handler for task type '${taskType}'`);
}

/**
 * Processa uma tarefa
 * @param {Object} task - Tarefa a ser processada
 * @returns {Promise<*>} - Resultado da tarefa
 */
async function processTask(task) {
  const { taskId, taskType, data } = task;
  
  console.log(`Worker ${workerId}: Processing task ${taskId} of type ${taskType}`);
  
  const startTime = performance.now();
  
  try {
    // Verifica se existe um handler para este tipo de tarefa
    if (!taskHandlers[taskType]) {
      throw new Error(`No handler registered for task type '${taskType}'`);
    }
    
    // Executa o handler
    const result = await taskHandlers[taskType](data);
    
    const endTime = performance.now();
    const processingTime = endTime - startTime;
    
    // Atualiza estatísticas
    stats.tasksProcessed++;
    stats.tasksSucceeded++;
    stats.totalProcessingTime += processingTime;
    
    // Envia o resultado para o thread principal
    parentPort.postMessage({
      type: 'result',
      taskId,
      result,
      processingTime
    });
    
    return result;
  } catch (error) {
    const endTime = performance.now();
    const processingTime = endTime - startTime;
    
    // Atualiza estatísticas
    stats.tasksProcessed++;
    stats.tasksFailed++;
    stats.totalProcessingTime += processingTime;
    
    // Envia o erro para o thread principal
    parentPort.postMessage({
      type: 'error',
      taskId,
      error: error.message || 'Unknown error',
      stack: error.stack,
      processingTime
    });
    
    throw error;
  }
}

// Configura o listener para mensagens do thread principal
parentPort.on('message', async (message) => {
  if (message.type === 'task') {
    // Processa a tarefa
    try {
      await processTask(message);
    } catch (error) {
      // Erros já são tratados em processTask
    }
  } else if (message.type === 'shutdown') {
    // Encerra o worker graciosamente
    console.log(`Worker ${workerId}: Received shutdown signal`);
    
    // Envia estatísticas finais
    sendStats();
    
    // Limpa o intervalo de estatísticas
    if (statsIntervalId) {
      clearInterval(statsIntervalId);
    }
    
    // Não é necessário chamar process.exit(), o worker será encerrado
    // quando o thread principal chamar worker.terminate()
  }
});

/**
 * Envia estatísticas para o thread principal
 */
function sendStats() {
  const currentStats = {
    ...stats,
    uptime: Date.now() - stats.startTime,
    avgProcessingTime: stats.tasksProcessed > 0 
      ? stats.totalProcessingTime / stats.tasksProcessed 
      : 0,
    memoryUsage: process.memoryUsage()
  };
  
  parentPort.postMessage({
    type: 'stats',
    stats: currentStats
  });
}

// Configura o envio periódico de estatísticas
const statsIntervalId = setInterval(sendStats, statsInterval);

// Registra handlers para tipos de tarefas específicos
// Estes são apenas exemplos, os handlers reais serão registrados com base nas necessidades do LogPipe
registerTaskHandler('transform', async (data) => {
  // Exemplo: aplica uma transformação a um registro
  if (!data || !data.record) {
    throw new Error('Invalid data for transform task: record is required');
  }
  
  // Exemplo simples de transformação
  const result = { ...data.record };
  
  // Aplica a transformação especificada
  if (data.transformType === 'addTimestamp') {
    result.processedAt = new Date().toISOString();
  } else if (data.transformType === 'uppercase') {
    // Converte todas as strings para maiúsculas
    Object.keys(result).forEach(key => {
      if (typeof result[key] === 'string') {
        result[key] = result[key].toUpperCase();
      }
    });
  }
  
  return result;
});

registerTaskHandler('filter', async (data) => {
  // Exemplo: filtra um registro com base em critérios
  if (!data || !data.record || !data.criteria) {
    throw new Error('Invalid data for filter task: record and criteria are required');
  }
  
  // Implementação simples de filtragem
  for (const [key, value] of Object.entries(data.criteria)) {
    if (data.record[key] !== value) {
      return false; // Não corresponde aos critérios
    }
  }
  
  return true; // Corresponde a todos os critérios
});

registerTaskHandler('hash', async (data) => {
  // Exemplo: calcula um hash para um valor (operação intensiva de CPU)
  if (!data || !data.value) {
    throw new Error('Invalid data for hash task: value is required');
  }
  
  // Simulação de uma operação intensiva de CPU
  const crypto = require('crypto');
  
  // Número de iterações para tornar a operação mais intensiva
  const iterations = data.iterations || 10000;
  
  let hash = data.value;
  
  // Calcula o hash várias vezes para simular carga de CPU
  for (let i = 0; i < iterations; i++) {
    hash = crypto.createHash('sha256').update(hash).digest('hex');
  }
  
  return hash;
});

// Notifica o thread principal que o worker está pronto
parentPort.postMessage({ type: 'ready' });
console.log(`Worker ${workerId}: Initialized and ready`);
