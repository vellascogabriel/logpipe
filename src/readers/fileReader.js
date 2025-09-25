const { pipeline } = require('stream');
const { promisify } = require('util');
const path = require('path');
const fs = require('fs');
const zlib = require('zlib');
const logger = require('../utils/logger');

const pipelineAsync = promisify(pipeline);

/**
 * Create a readable stream for a file, with automatic decompression if necessary
 * @param {string} filePath - Path to the file to be read
 * @param {Object} options - Options for creating the stream
 * @param {number} options.start - Byte offset to start reading from (for resuming)
 * @returns {Promise<stream.Readable>} - Readable stream
 */
async function createReadStream(filePath, options = {}) {
    if (!fs.existsSync(filePath)) {
        throw new Error(`File does not exist: ${filePath}`);
    }

    // Determine if the file is compressed according to its extension
    const isGzipped = path.extname(filePath).toLowerCase() === '.gz';

    // Create the read stream with start position if specified
    const readOptions = {};
    if (options.start && options.start > 0) {
        readOptions.start = options.start;
        logger.info(`Resuming file read from byte offset: ${options.start}`);
    }

    const readStream = fs.createReadStream(filePath, readOptions);

    if (isGzipped) {
        const gunzipStream = zlib.createGunzip();
        // Connect the streams and return the output stream
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
 * @param {Object} options - Additional options
 * @param {Object} options.checkpoint - Checkpoint information for resuming
 * @returns {Promise<void>}
 */
async function processFile(inputPath, outputStream, transformers = [], options = {}) {
    try {
        // Create the read stream with checkpoint information if available
        const readOptions = {};
        if (options.checkpoint && options.checkpoint.offset > 0) {
            readOptions.start = options.checkpoint.offset;
        }

        const readStream = await createReadStream(inputPath, readOptions);

        // If there are no transformers, just pipe the read stream to the output stream
        if (transformers.length === 0) {
            await pipelineAsync(readStream, outputStream);
            return;
        }

        // Create the stream pipeline
        const streams = [readStream, ...transformers, outputStream];
        await pipelineAsync(...streams);

    } catch (error) {
        logger.error({ error: error.message }, 'Error processing file');
        throw error;
    }
}

module.exports = {
    createReadStream,
    processFile
};