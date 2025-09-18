const http = require('http');
const fs = require('fs');
const path = require('path');

// Configurações do servidor
const PORT = process.env.PORT || 3000;
const OUTPUT_DIR = path.join(__dirname, '../../test-output');

// Cria o diretório de saída se não existir
if (!fs.existsSync(OUTPUT_DIR)) {
  fs.mkdirSync(OUTPUT_DIR, { recursive: true });
}

// Contador de requisições
let requestCount = 0;

// Cria o servidor HTTP
const server = http.createServer((req, res) => {
  // Incrementa o contador de requisições
  requestCount++;
  
  // Registra informações sobre a requisição
  const timestamp = new Date().toISOString();
  const requestId = `req_${timestamp.replace(/[:.]/g, '')}_${requestCount}`;
  
  console.log(`[${timestamp}] Received request #${requestCount} (${requestId}): ${req.method} ${req.url}`);
  
  // Verifica se é uma requisição POST ou PUT para /logs
  if ((req.method === 'POST' || req.method === 'PUT') && req.url.startsWith('/logs')) {
    // Coleta o corpo da requisição
    let body = [];
    req.on('data', (chunk) => {
      body.push(chunk);
    });
    
    req.on('end', () => {
      try {
        // Converte o corpo para string
        body = Buffer.concat(body).toString();
        
        // Tenta fazer parse do JSON
        const data = JSON.parse(body);
        
        // Verifica se é um array
        if (!Array.isArray(data)) {
          console.error(`[${timestamp}] Error: Expected array, got ${typeof data}`);
          res.statusCode = 400;
          res.end(JSON.stringify({ error: 'Expected array of records' }));
          return;
        }
        
        // Registra o número de registros recebidos
        console.log(`[${timestamp}] Received ${data.length} records in batch`);
        
        // Salva os dados em um arquivo para inspeção
        const outputFile = path.join(OUTPUT_DIR, `${requestId}.json`);
        fs.writeFileSync(outputFile, JSON.stringify(data, null, 2));
        console.log(`[${timestamp}] Saved batch to ${outputFile}`);
        
        // Responde com sucesso
        res.statusCode = 200;
        res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify({
          success: true,
          recordsReceived: data.length,
          requestId
        }));
      } catch (error) {
        // Erro ao processar o JSON
        console.error(`[${timestamp}] Error processing request: ${error.message}`);
        res.statusCode = 400;
        res.end(JSON.stringify({ error: 'Invalid JSON data' }));
      }
    });
  } else if (req.method === 'GET' && req.url === '/status') {
    // Endpoint de status
    res.statusCode = 200;
    res.setHeader('Content-Type', 'application/json');
    res.end(JSON.stringify({
      status: 'ok',
      uptime: process.uptime(),
      requestsProcessed: requestCount,
      timestamp: new Date().toISOString()
    }));
  } else {
    // Rota não encontrada
    res.statusCode = 404;
    res.end(JSON.stringify({ error: 'Not found' }));
  }
});

// Inicia o servidor
server.listen(PORT, () => {
  console.log(`Test server running at http://localhost:${PORT}`);
  console.log(`Send data to http://localhost:${PORT}/logs`);
  console.log(`Check server status at http://localhost:${PORT}/status`);
  console.log(`Output files will be saved to ${OUTPUT_DIR}`);
});

// Manipuladores de eventos para encerramento gracioso
process.on('SIGINT', () => {
  console.log('Shutting down server...');
  server.close(() => {
    console.log('Server shut down successfully');
    process.exit(0);
  });
});

process.on('SIGTERM', () => {
  console.log('Shutting down server...');
  server.close(() => {
    console.log('Server shut down successfully');
    process.exit(0);
  });
});
