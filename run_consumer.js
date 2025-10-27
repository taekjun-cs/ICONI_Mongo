// This script runs on VM 5 (Consumer)
// It opens a Change Stream and 'processes' events in batches.

const { MongoClient } = require('mongodb');
const yargs = require('yargs/yargs');
const { hideBin } = require('yargs/helpers');

// --- MongoDB Connection ---
// Use the INTERNAL IPs of your 3 DB VMs
const MONGO_URI = 'mongodb://10.142.0.2:27017,10.142.0.3:27017,10.142.0.4:27017/?replicaSet=rs0';
const DB_NAME = 'experimentDB';
const COLLECTION_NAME = 'documents';

// --- Argument Parsing ---
const argv = yargs(hideBin(process.argv))
  .option('fullDocument', {
    alias: 'f',
    type: 'string',
    description: "Change Stream 'fullDocument' option",
    choices: ['default', 'updateLookup'],
    demandOption: true,
  })
  .option('batchSize', {
    alias: 'b',
    type: 'number',
    description: 'Number of events to process in one batch',
    demandOption: true,
  })
  .argv;

let client;
let changeStream;
let eventBatch = [];
let totalEventsProcessed = 0;
let totalProcessingTimeMs = 0;

// This function simulates the work of processing a batch.
// This is where your application logic (e.g., writing to another system) would go.
async function processBatch(batch) {
  const batchStartTime = process.hrtime.bigint();
  const db = client.db(DB_NAME);
  const collection = db.collection(COLLECTION_NAME);

  try {
    // --- This is the CORE logic of your experiment ---
    if (argv.fullDocument === 'default') {
      // 'default' = We only got the _id. We must fetch the full docs ourselves.
      const docIds = batch.map(event => event.documentKey._id);
      
      // Simulate fetching all documents in the batch
      const docs = await collection.find({ _id: { $in: docIds } }).toArray();
      
      // (Optional) Simulate further processing
      // for (const doc of docs) { /* do work */ }

    } else {
      // 'updateLookup' = The full doc is already in event.fullDocument.
      // We simulate processing it directly.
      const docs = batch.map(event => event.fullDocument);

      // (Optional) Simulate further processing
      // for (const doc of docs) { /* do work */ }
    }
    // --- End of CORE logic ---

    const batchEndTime = process.hrtime.bigint();
    const batchTimeMs = Number(batchEndTime - batchStartTime) / 1_000_000;
    
    totalEventsProcessed += batch.length;
    totalProcessingTimeMs += batchTimeMs;

    console.log(`[CONSUMER] Processed batch of ${batch.length}. Time: ${batchTimeMs.toFixed(2)} ms. Total events: ${totalEventsProcessed}`);

  } catch (err) {
    console.error(`[CONSUMER] Error processing batch:`, err);
  }
}

async function startConsumer() {
  client = new MongoClient(MONGO_URI);
  try {
    await client.connect();
    const db = client.db(DB_NAME);
    const collection = db.collection(COLLECTION_NAME);

    console.log(`[CONSUMER] Connected to MongoDB.`);
    console.log(`[CONSUMER] Config: fullDocument = '${argv.fullDocument}', batchSize = ${argv.batchSize}`);
    console.log(`[CONSUMER] Dropping old collection...`);
    
    // Clear the collection for a clean experiment run
    try {
      await collection.drop();
    } catch (dropErr) {
      if (dropErr.codeName !== 'NamespaceNotFound') {
        throw dropErr;
      }
    }
    console.log(`[CONSUME] Collection dropped. Waiting for new events...`);


    // --- [E]-2-a: Setup Change Stream ---
    const changeStreamOptions = {
      fullDocument: argv.fullDocument
    };
    
    // We only care about 'update' operations for this experiment
    const pipeline = [
      { $match: { operationType: 'update' } }
    ];
    
    changeStream = collection.watch(pipeline, changeStreamOptions);

    // Start listening
    changeStream.on('change', (event) => {
      eventBatch.push(event);

      // When batch is full, process it
      if (eventBatch.length >= argv.batchSize) {
        const batchToProcess = [...eventBatch];
        eventBatch = []; // Clear the batch immediately
        processBatch(batchToProcess); // Process asynchronously
      }
    });

    changeStream.on('error', (err) => {
      console.error('[CONSUMER] Change Stream Error:', err);
    });

    changeStream.on('close', () => {
      console.log('[CONSUMER] Change Stream closed.');
      // Process any remaining events
      if (eventBatch.length > 0) {
        console.log(`[CONSUMER] Processing final batch of ${eventBatch.length}...`);
        processBatch(eventBatch);
      }
      
      // --- [E]-2-e: Calculate Final Metrics ---
      console.log(`--- [CONSUMER] FINAL RESULTS ---`);
      console.log(`Scenario: fullDocument=${argv.fullDocument}, batchSize=${argv.batchSize}`);
      console.log(`Total Events Processed: ${totalEventsProcessed}`);
      console.log(`Total Processing Time: ${totalProcessingTimeMs.toFixed(2)} ms`);
      
      // M1: Throughput (ops/sec)
      const M1_Throughput = (totalEventsProcessed / totalProcessingTimeMs) * 1000;
      console.log(`M1 - Throughput: ${M1_Throughput.toFixed(2)} ops/sec`);

      // M2: Average Batch Latency (ms)
      const numBatches = totalEventsProcessed / argv.batchSize;
      const M2_AvgBatchLatency = totalProcessingTimeMs / numBatches;
      console.log(`M2 - Avg. Batch Latency: ${M2_AvgBatchLatency.toFixed(2)} ms`);
      console.log(`--- [CONSUMER] END ---`);
      
      client.close();
    });

    // We need a way to stop the consumer.
    // For this experiment, we'll assume the generator finishes and the stream idles.
    // A better way would be a signal or timeout.
    // Let's set a timeout after generator finishes (e.g., 30s idle)
    let idleTimeout = setTimeout(stopConsumer, 30000); // 30s idle timeout
    changeStream.on('change', () => {
      clearTimeout(idleTimeout);
      idleTimeout = setTimeout(stopConsumer, 30000); // Reset timeout on new event
    });


  } catch (err) {
    console.error('[CONSUMER] Error:', err);
    if (client) {
      await client.close();
    }
  }
}

function stopConsumer() {
  console.log('[CONSUMER] No events received for 30 seconds. Shutting down consumer.');
  if (changeStream) {
    changeStream.close();
  }
}

startConsumer();
