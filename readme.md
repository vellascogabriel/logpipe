# LogPipe

LogPipe é uma ferramenta de linha de comando para processamento eficiente de grandes arquivos de log, com suporte a transformações, filtragem e envio de dados para endpoints HTTP.

## Características

- Processamento de arquivos grandes com uso eficiente de memória usando streams
- Suporte a múltiplos formatos de entrada (NDJSON, CSV)
- Transformações de dados (filtro, mapeamento, agregação)
- Processamento paralelo com worker threads
- Envio de dados em lotes para endpoints HTTP
- Tratamento robusto de erros e retentativas
- Monitoramento de progresso em tempo real

## Instalação

```bash
# Clone o repositório
git clone https://github.com/seu-usuario/logpipe.git
cd logpipe

# Instale as dependências
npm install

# Torne o comando executável
npm link
```

## Uso Básico

```bash
# Processar um arquivo NDJSON e exibir na saída padrão
logpipe -i arquivo.json

# Processar um arquivo CSV e salvar em um arquivo de saída
logpipe -i arquivo.csv -o saida.json --format csv

# Filtrar registros por campo
logpipe -i arquivo.json --filter "level:error"

# Selecionar apenas campos específicos
logpipe -i arquivo.json --select "timestamp,message,level"

# Contar registros por campo
logpipe -i arquivo.json --count-by "level"

# Calcular estatísticas para um campo numérico
logpipe -i arquivo.json --stats "level:responseTime"
```

## Processamento Paralelo

LogPipe suporta processamento paralelo usando worker threads para melhorar o desempenho em tarefas intensivas de CPU:

```bash
# Usar processamento paralelo com o número padrão de workers (igual ao número de CPUs)
logpipe -i arquivo.json --parallel

# Especificar o número de workers
logpipe -i arquivo.json --parallel -w 4

# Calcular hash para um campo (sempre usa workers por ser CPU intensivo)
logpipe -i arquivo.json --hash-field "id"
```

## Checkpoints e Retomada de Processamento

LogPipe pode salvar o progresso periodicamente e retomar o processamento a partir do último ponto em caso de interrupção:

```bash
# Habilitar checkpoints
logpipe -i arquivo.json -c checkpoint.json

# Especificar o intervalo de salvamento de checkpoints (em ms)
logpipe -i arquivo.json -c checkpoint.json --checkpoint-interval 60000

# Retomar o processamento a partir de um checkpoint existente
# (use o mesmo comando que foi interrompido)
logpipe -i arquivo.json -c checkpoint.json
```

Quando o processamento é interrompido (por exemplo, com Ctrl+C), o LogPipe salva o estado atual em um arquivo de checkpoint. Na próxima execução, ele detecta automaticamente que deve retomar o processamento a partir do último ponto salvo.

## Análise de Performance (Profiling)

LogPipe inclui ferramentas de profiling para analisar o desempenho do processamento:

```bash
# Habilitar profiling
logpipe -i arquivo.json -p

# Especificar o diretório para relatórios de profiling
logpipe -i arquivo.json -p --profile-dir "./profiling-reports"

# Especificar o intervalo de coleta de métricas (em ms)
logpipe -i arquivo.json -p --profile-interval 10000
```

O profiler coleta métricas de:
- Uso de CPU
- Uso de memória
- Eventos de garbage collection
- Tempo de execução de cada etapa do processamento

Os relatórios são salvos em arquivos JSON no diretório de profiling e incluem tanto dados detalhados quanto um resumo das métricas coletadas.

## Envio para Endpoint HTTP

LogPipe pode enviar dados processados para um endpoint HTTP em lotes:

```bash
# Enviar dados para um endpoint HTTP
logpipe -i arquivo.json --http-endpoint "http://localhost:3000/logs"

# Especificar o método HTTP (padrão: POST)
logpipe -i arquivo.json --http-endpoint "http://localhost:3000/logs" --http-method "PUT"

# Configurar o tamanho do lote (padrão: 100)
logpipe -i arquivo.json --http-endpoint "http://localhost:3000/logs" --http-batch-size 50

# Configurar retentativas e timeout
logpipe -i arquivo.json --http-endpoint "http://localhost:3000/logs" --http-retries 5 --http-timeout 60000

# Adicionar cabeçalhos HTTP personalizados
logpipe -i arquivo.json --http-endpoint "http://localhost:3000/logs" --http-headers '{"Authorization": "Bearer token123"}'
```

## Servidor de Teste

O projeto inclui um servidor HTTP de teste para facilitar o desenvolvimento e testes:

```bash
# Iniciar o servidor de teste
node src/test/testServer.js

# Em outro terminal, enviar dados para o servidor
logpipe -i arquivo.json --http-endpoint "http://localhost:3000/logs"
```

O servidor de teste salva os lotes recebidos em arquivos JSON no diretório `test-output/` para inspeção.

## Opções Completas

```
Options:
  -V, --version                output the version number
  -i, --input <path>           Input log file path
  -o, --output <path>          Output log file path
  -f, --format <format>        File format (ndjson, csv) (default: "ndjson")
  -b, --batch-size <size>      Batch size for processing (default: "1000")
  -w, --workers <count>        Number of worker processes (default: number of CPUs)
  -p, --profile                Enable performance profiling
  -c, --checkpoint <path>      Checkpoint file to resume processing
  --csv-separator <char>       CSV separator character (default: ",")
  --csv-header <boolean>       CSV has header row (default: true)
  --filter <field:value>       Filter records by field value
  --select <fields>            Select only specified fields (comma-separated)
  --count-by <field>           Count records by field
  --stats <keyField:valueField> Calculate statistics for numeric field grouped by key
  --pretty-output              Format JSON output with indentation (default: false)
  --parallel                   Use worker threads for parallel processing (default: false)
  --hash-field <field>         Calculate hash for specified field (CPU intensive)
  --enrich                     Add processing metadata to records (default: false)
  --http-endpoint <url>        HTTP endpoint URL to send data to
  --http-method <method>       HTTP method (POST, PUT) (default: "POST")
  --http-batch-size <size>     Number of records to send in each HTTP request (default: "100")
  --http-retries <count>       Number of retries for failed HTTP requests (default: "3")
  --http-timeout <ms>          HTTP request timeout in milliseconds (default: "30000")
  --http-headers <headers>     HTTP headers in JSON format
  -h, --help                   display help for command
```

## Exemplos de Uso Avançado

### Pipeline de ETL Completo

```bash
# Extrair dados de um arquivo CSV, transformar e enviar para um endpoint HTTP
logpipe -i logs.csv --format csv \
  --filter "status:error" \
  --select "timestamp,message,errorCode,userId" \
  --enrich \
  --http-endpoint "https://api.example.com/logs" \
  --http-headers '{"X-API-Key": "your-api-key"}'
```

### Análise de Logs com Agregação

```bash
# Contar erros por código de erro e salvar em um arquivo
logpipe -i logs.json \
  --filter "level:error" \
  --count-by "errorCode" \
  --pretty-output \
  -o error-summary.json
```

### Processamento de Arquivos Grandes com Paralelismo

```bash
# Processar um arquivo grande com worker threads e enviar para HTTP
logpipe -i huge-logs.json \
  --parallel \
  -w 8 \
  --batch-size 5000 \
  --http-endpoint "http://localhost:3000/logs" \
  --http-batch-size 200
```

## Licença

MIT