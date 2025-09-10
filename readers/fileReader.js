const { pipeline } = require('stream');
const { promisify } = require('util');
const path = require('path');
const fs = require('fs');
const zlib = require('zlib');

const pipelineAsync = promisify(pipeline);

/**
 * Create a readable stream for a file, with automatic decompression if necessary
 * @param {string} filePath - Path to the file to be read
 * @returns {Promise<stream.Readable>} - Readable stream
 */

async function createReadStream(filePath){
    if(!fs.existsSync(filePath)){
        throw new Error('File does not exist');
    }

    //Determine if the file is compressed according to its extension
    const isGzipped = path.extname(filePath).toLowerCase() === '.gz';

    // Create the read stream
    const readStream = fs.createReadStream(filePath);

    if(isGzipped){
        const gunzipStream = zlib.createGunzip();
        //Connect the streams and return the output stream
        readStream.pipe(gunzipStream);
        return gunzipStream;
    }

    return readStream;
}

/**
 * Process a log file using streams
 * @param {string} inputPath - Path to the input file
 * @param {stream.Writable} outputStream - Output stream (can be a file or another destination)
 * @param {stream.Transform[]} transformers - Array of transformers to be applied
 * @returns {Promise<void>}
 */
async function processFile(inputPath, outputStream, transformers = []){
    try{
        //Create the read stream
        const readStream = await createReadStream(inputPath);

        //If there are no transformers, just pipe the read stream to the output stream
        if(transformers.length === 0){
            await pipelineAsync(readStream, outputStream);
            return;
        }

        //Create the stream pipeline
        const streams = [readStream, ...transformers, outputStream];
        await pipelineAsync(...streams);

    } catch (error) {
        console.error('Error ao processar arquivo:', error);
        throw error;
    }
}


module.exports = {
    createReadStream,
    processFile
}