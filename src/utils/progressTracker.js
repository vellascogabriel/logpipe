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

    /**
     * Relata o progresso atual do processamento
     */
    reportProgress() {
        const elapsedSeconds = (Date.now() - this.startTime) / 1000;
        const mbProcessed = this.bytesProcessed / (1024 * 1024);
        const mbTotal = this.fileSize / (1024 * 1024);
        const percentComplete = this.fileSize > 0 ? (this.bytesProcessed / this.fileSize) * 100 : 0;
        const mbPerSecond = mbProcessed / elapsedSeconds;

        logger.info({
            file: this.filePath,
            processed: `${mbProcessed.toFixed(2)} MB`,
            total: `${mbTotal.toFixed(2)} MB`,
            percent: `${percentComplete.toFixed(2)}%`,
            speed: `${mbPerSecond.toFixed(2)} MB/s`,
            elapsed: `${elapsedSeconds.toFixed(2)}s`
        }, 'Processing progress');
    }

    /**
     * Finaliza o rastreamento e exibe o relatório final
     */
    finish() {
        if (this.finished) return;
        
        this.finished = true;
        this.reportProgress();
        
        const totalTime = (Date.now() - this.startTime) / 1000;
        logger.info({
            file: this.filePath,
            totalProcessed: `${(this.bytesProcessed / (1024 * 1024)).toFixed(2)} MB`,
            totalTime: `${totalTime.toFixed(2)}s`
        }, 'Processing completed');
    }
}

module.exports = ProgressTracker;