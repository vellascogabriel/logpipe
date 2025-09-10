module.exports = {
    processing: {
        batchSize: 1000,
        workers: 1,
        checkpointInterval: 5000,
    },

    //Logging configuration
    logger: {
        level: process.env.LOG_LEVEL || 'info',
        prettyPrint: process.env.NODE_ENV !== 'production',
    },

    //Configuring the HTTP (to send data)
    http: {
        endpoint: process.env.HTTP_ENDPOINT || 'http://localhost:3000/logs',
        timeout: process.env.HTTP_TIMEOUT || 5000,
        retries: process.env.HTTP_RETRIES || 3
    }
};