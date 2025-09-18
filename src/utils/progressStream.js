const { Transform } = require('stream');
const ProgressTracker = require('./progressTracker');


class ProgressStream extends Transform {

    constructor(filePath, reportInterval = 5000, options = {}){
        super(options);
        this.progressTracker = new ProgressTracker(filePath, reportInterval);

        this.on('end', () => {
            this.progressTracker.finish();
        })
    }

    /**
   * Implementação do método _transform exigido pela interface Transform
   * @param {Buffer|string} chunk - Pedaço de dados a ser processado
   * @param {string} encoding - Codificação do chunk (ignorado se chunk for Buffer)
   * @param {function} callback - Função de callback a ser chamada quando terminar
   */

    _transform(chunck, encoding, callback){
        this.tracker.update(chunck.length);

        this.push(chunck);
        callback();
    }

     /**
     * Implementação do método _flush exigido pela interface Transform
     * @param {function} callback - Função de callback a ser chamada quando terminar
     */
    _flush(callback){
        this.tracker.finish();
        callback();
    }
}

module.exports = ProgressStream;