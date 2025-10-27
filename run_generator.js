// This script runs on VM 4 (Generator)
// It seeds the database and then generates update workloads.

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
  .option('numDocs', {
    alias: 'n',
    type: 'number',
    description: 'Total number of documents to insert and then update',
    demandOption: true,
  })
  .option('docSize', {
    alias: 's',
    type: 'number',
    description: 'Size of each document in KB',
    demandOption: true,
  })
  .argv;

// Function to generate a random payload of approx. size in KB
function generatePayload(sizeKB) {
  // 1 KB = 1024 bytes.
  // Using hex encoding, 1 byte = 2 chars. So we need sizeKB * 512 chars.
  const chars = 'abcdefghijklmnopqrstuvwxyz0123456789';
  let payload = '';
  const targetLength = sizeKB * 512;
  for (let i = 0; i < targetLength; i++) {
    payload += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return payload;
}

async function runExperiment() {
  const client = new MongoClient(MONGO_URI);
  try {
    await client.connect();
    const db = client.db(DB_NAME);
    const collection = db.collection(COLLECTION_NAME);

    console.log(`[GENERATOR] Connected to MongoDB. Starting experiment...`);
    console.log(`[GENERATOR] Config: numDocs = ${argv.numDocs}, docSize = ${argv.docSize} KB`);

    // --- [E]-1-b: Data Seeding (Phase 1) ---
    console.log(`[GENERATOR] Phase 1: Seeding ${argv.numDocs} documents...`);
    const payload = generatePayload(argv.docSize);
    const docIds = [];

    for (let i = 0; i < argv.numDocs; i++) {
      const doc = {
        payload: payload,
        counter: 0,
        lastUpdated: new Date()
      };
      const result = await collection.insertOne(doc);
      docIds.push(result.insertedId);
      if ((i + 1) % 1000 === 0) {
        console.log(`[GENERATOR] Seeded ${i + 1}/${argv.numDocs} documents...`);
      }
    }
    console.log(`[GENERATOR] Phase 1: Seeding complete.`);

    // --- [E]-2-b: Workload Generation (Phase 2) ---
    console.log(`[GENERATOR] Phase 2: Starting update workload... (Waiting 5s for consumer to catch up)`);
    await new Promise(resolve => setTimeout(resolve, 5000)); // Wait for consumer to be ready

    const startTime = process.hrtime.bigint();
    let updatesDone = 0;

    for (const id of docIds) {
      await collection.updateOne(
        { _id: id },
        {
          $set: { lastUpdated: new Date() },
          $inc: { counter: 1 }
        }
      );
      updatesDone++;
      if (updatesDone % 1000 === 0) {
        console.log(`[GENERATOR] Updated ${updatesDone}/${argv.numDocs} documents...`);
      }
    }

    const endTime = process.hrtime.bigint();
    const totalTimeMs = Number(endTime - startTime) / 1_000_000;
    const throughput = (argv.numDocs / totalTimeMs) * 1000; // ops/sec

    console.log(`[GENERATOR] Phase 2: Update workload complete.`);
    console.log(`--- [GENERATOR] RESULT ---`);
    console.log(`Total Updates: ${argv.numDocs}`);
    console.log(`Total Time: ${totalTimeMs.toFixed(2)} ms`);
    console.log(`Generator Throughput: ${throughput.toFixed(2)} ops/sec`);
    console.log(`--- [GENERATOR] END ---`);


  } catch (err) {
    console.error('[GENERATOR] Error:', err);
  } finally {
    await client.close();
    console.log('[GENERATOR] Disconnected from MongoDB.');
  }
}

runExperiment();
