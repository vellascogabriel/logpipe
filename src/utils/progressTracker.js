const fs = require('fs');
const logger = require('./logger');


/**
 * Class for tracking file processing progress
 */
class ProgressTracker {
    /**
   * Creates a new instance of the progress tracker
   * @param {string} filePath - Path of the file being processed
   * @param {number} reportInterval - Interval in ms to report progress
   */
    constructor(filePath, reportInterval = 5000) {
        this.filePath = filePath;
        this.reportInterval = reportInterval;
        this.bytesProcessed = 0;
        this.startTime = Date.now();
        this.lastReportTime = this.startTime;
        this.fileSize = 0;
        this.finished = false;

        try {
            const stats = fs.statSync(filePath);
            this.fileSize = stats.size;
        } catch (error) {
            logger.warn(`Could not determine file size for ${filePath}`);
        }        
    }

    /**
   * Atualiza o progresso com a quantidade de bytes processados
   * @param {number} bytes - Número de bytes processados desde a última atualização
   */
    update(bytes) {
        this.bytesProcessed += bytes;
        const now = Date.now();

        if(now - this.lastReportTime >= this.reportInterval){
            this.reportProgress();
            this.lastReportTime = now;
            
        }
    }
}