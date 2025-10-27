/**
 * 워크로드 생성기 (v6 - 최종 수정)
 * - Phase 1(Seeding)의 .drop()이 .watch() 커서를 닫아버리는 치명적 오류 수정
 * - .drop()을 .deleteMany({})로 대체하여 Consumer 스트림 유지
 * - (v5) Phase 1(Seeding)의 '잘못된 컬렉션' 지정 오류 수정
 * - (v5) Phase 1(Seeding)의 '메모리 부족(Out of Memory)' 오류 수정
 * - (v5) Phase 2(Update)는 v3의 'Throttled' 로직 유지
 * - (v-user) MONGO_URI의 포트 오타 수정 (2017 -> 27017)
 */

const { MongoClient } = require('mongodb');
const yargs = require('yargs/yargs');
const { hideBin } = require('yargs/helpers');

// [!!! v6 수정 !!!] 10.142.0.4의 포트 오타 2017 -> 27017로 수정
// [!!! 중요 !!!] 이 URI의 <...> 부분을 당신의 GCP '내부 IP'로 수정하십시오!
const MONGO_URI = 'mongodb://10.142.0.2:27017,10.142.0.3:27017,10.142.0.4:27017/?replicaSet=rs0';

const DB_NAME = 'testdb';
const COLLECTION_NAME = 'testcoll'; // [!!!] 컬렉션 이름은 'testcoll' 입니다.

// 커맨드라인 인자 파싱 (이전과 동일)
const argv = yargs(hideBin(process.argv))
  .option('numDocs', {
    alias: 'n',
    type: 'number',
    description: '생성할 총 문서 수',
    demandOption: true,
  })
  .option('docSize', {
    alias: 's',
    type: 'number',
    description: '문서 크기 (KB)',
    demandOption: true,
  })
  .argv;

// 페이로드 생성 (이전과 동일)
function generatePayload(sizeKB) {
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  const charLength = 1024; // 1KB
  let payload = '';
  for (let i = 0; i < sizeKB; i++) {
    for (let j = 0; j < charLength; j++) {
      payload += chars.charAt(Math.floor(Math.random() * chars.length));
    }
  }
  return payload;
}


// 1. Phase 1: Seeding (Batched)
async function seedData(client, numDocs, docSize) {
  console.log(`[GENERATOR] --- PHASE 1 START ---`);
  console.log(`[GENERATOR] Seeding ${numDocs} documents of ${docSize}KB each... (Batch mode)`);
  const db = client.db(DB_NAME);
  const collection = db.collection(COLLECTION_NAME); // (v5에서 수정됨)

  // [!!! v6 수정 !!!]
  try {
    // .drop()은 consumer의 change stream을 닫아버림.
    // .deleteMany({})로 대체하여 커서를 유지시킴.
    console.log(`[GENERATOR] Deleting old documents...`);
    const deleteResult = await collection.deleteMany({});
    console.log(`[GENERATOR] Deleted ${deleteResult.deletedCount} old documents.`);
  } catch (err) {
    // deleteMany는 컬렉션이 없어도 심각한 오류를 뱉지 않음.
    console.error('[GENERATOR] Warning during deleteMany (this is usually safe):', err.message);
  }
  // [!!! v6 수정 끝 !!!]


  const INSERT_BATCH_SIZE = 1000; // (v4에서 수정됨)
  const payload = generatePayload(docSize);
  let totalInserted = 0;

  for (let i = 0; i < numDocs; i += INSERT_BATCH_SIZE) {
    const docs = []; 
    const end = Math.min(i + INSERT_BATCH_SIZE, numDocs);

    for (let j = i; j < end; j++) {
      docs.push({
        docId: j,
        payload: payload,
        version: 0,
      });
    }
    
    await collection.insertMany(docs); // (v4에서 수정됨)
    totalInserted += docs.length;

    console.log(`[GENERATOR] Inserted docs ${i} to ${end - 1} (Total: ${totalInserted}/${numDocs})`);
  }
  
  console.log(`[GENERATOR] --- PHASE 1 COMPLETE --- (${totalInserted} docs inserted)`);
}

// 2. Phase 2: Update Workload (v3/v4/v5 로직 그대로 유지 - 문제 없음)
async function runUpdateWorkload(client, numDocs) {
  console.log(`[GENERATOR] --- PHASE 2 START ---`);
  console.log(`[GENERATOR] Starting update workload for ${numDocs} documents...`);
  const db = client.db(DB_NAME);
  const collection = db.collection(COLLECTION_NAME); 

  const UPDATE_BATCH_SIZE = 1000; 
  let totalUpdated = 0;

  for (let i = 0; i < numDocs; i += UPDATE_BATCH_SIZE) {
    const updatePromises = [];
    const end = Math.min(i + UPDATE_BATCH_SIZE, numDocs);

    for (let j = i; j < end; j++) {
      updatePromises.push(
        collection.updateOne(
          { docId: j },
          { $set: { version: 1 } }
        )
      );
    }
    
    await Promise.all(updatePromises);
    totalUpdated += updatePromises.length;
    
    console.log(`[GENERATOR] Updated docs ${i} to ${end - 1} (Total: ${totalUpdated}/${numDocs})`);
  }
  
  console.log(`[GENERATOR] --- PHASE 2 COMPLETE --- (${totalUpdated} docs updated)`);
}

// 메인 실행 함수 (이전과 동일)
async function main() {
  const client = new MongoClient(MONGO_URI);
  try {
    await client.connect();
    console.log('[GENERATOR] Connected to MongoDB replica set.');

    const { numDocs, docSize } = argv;

    // [v6] 수정된 Phase 1 실행
    await seedData(client, numDocs, docSize);

    // [v3~v5] 수정된 Phase 2 실행
    await runUpdateWorkload(client, numDocs);

  } catch (err) {
    console.error('[GENERATOR] Error:', err);
  } finally {
    await client.close();
    console.log('[GENERATOR] Disconnected from MongoDB.');
  }
}

main();

