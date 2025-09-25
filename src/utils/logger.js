const pino = require('pino');
const config = require('../../config/default');


// Logger configuration based on project configuration
const loggerOptions = {
    level: config.logger.level,
    transport: config.logger.prettyPrint ? 
    {
        target: 'pino-pretty',
        options: {
            colorize: true,
            translate: 'SYS:standard',
            ignore: 'pid,hostname'
        }
    } : undefined
}

//Create logger instance
const logger = pino(loggerOptions);

module.exports = logger;

