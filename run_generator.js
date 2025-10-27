/**
 * 워크로드 생성기 (v5 - 최종 수정)
 * - Phase 1(Seeding)의 '잘못된 컬렉션' 지정 오류 수정
 * - (v4) Phase 1(Seeding)의 '메모리 부족(Out of Memory)' 오류 수정
 * - (v4) Phase 2(Update)는 v3의 'Throttled' 로직 유지
 */

const { MongoClient } = require('mongodb');
const yargs = require('yargs/yargs');
const { hideBin } = require('yargs/helpers');

// [!!! 중요 !!!] 이 URI의 <...> 부분을 당신의 GCP '내부 IP'로 수정하십시오!
const MONGO_URI = 'mongodb://<mongo-p_내부_IP>:27017,<mongo-s1_내부_IP>:27017,<mongo-s2_내부_IP>:27017/?replicaSet=rs0';

const DB_NAME = 'testdb';
const COLLECTION_NAME = 'testcoll'; // [!!!] 컬렉션 이름은 'testcoll' 입니다.

// 커맨드라인 인자 파싱 (v2/v3/v4와 동일)
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

// 페이로드 생성 (v2/v3/v4와 동일)
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

// [!!! v5 수정 !!!]
// 1. Phase 1: Seeding (Batched)
async function seedData(client, numDocs, docSize) {
  console.log(`[GENERATOR] --- PHASE 1 START ---`);
  console.log(`[GENERATOR] Seeding ${numDocs} documents of ${docSize}KB each... (Batch mode)`);
  const db = client.db(DB_NAME);
  // [!!! v5 수정 !!!] DB_NAME이 아니라 COLLECTION_NAME을 사용하도록 수정
  const collection = db.collection(COLLECTION_NAME);

  try {
    await collection.drop();
    console.log(`[GENERATOR] Dropped old collection.`);
  } catch (err) {
    if (err.codeName !== 'NamespaceNotFound') throw err;
  }

  const INSERT_BATCH_SIZE = 1000; // 한 번에 1000개씩만 메모리에 생성
  const payload = generatePayload(docSize);
  let totalInserted = 0;

  for (let i = 0; i < numDocs; i += INSERT_BATCH_SIZE) {
    const docs = []; // 1000개짜리 '작은' 배열 (매 루프마다 초기화)
    const end = Math.min(i + INSERT_BATCH_SIZE, numDocs);

    for (let j = i; j < end; j++) {
      docs.push({
        docId: j,
        payload: payload,
        version: 0,
      });
    }
    
    // 1000개 배치만 insertMany (메모리 문제 없음)
    await collection.insertMany(docs);
    totalInserted += docs.length;

    // 진행 상황 로깅
    console.log(`[GENERATOR] Inserted docs ${i} to ${end - 1} (Total: ${totalInserted}/${numDocs})`);
  }
  
  console.log(`[GENERATOR] --- PHASE 1 COMPLETE --- (${totalInserted} docs inserted)`);
}

// 2. Phase 2: Update Workload (v3/v4 로직 그대로 유지 - 문제 없음)
async function runUpdateWorkload(client, numDocs) {
  console.log(`[GENERATOR] --- PHASE 2 START ---`);
  console.log(`[GENERATOR] Starting update workload for ${numDocs} documents...`);
  const db = client.db(DB_NAME);
  const collection = db.collection(COLLECTION_NAME); // (여기는 v3/v4에서도 정상이었습니다)

  const UPDATE_BATCH_SIZE = 1000; // 한 번에 1000개씩만 병렬 처리
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

// 메인 실행 함수 (v2/v3/v4와 동일)
async function main() {
  const client = new MongoClient(MONGO_URI);
  try {
    await client.connect();
    console.log('[GENERATOR] Connected to MongoDB replica set.');

    const { numDocs, docSize } = argv;

    // [v5] 수정된 Phase 1 실행
    await seedData(client, numDocs, docSize);

    // [v3/v4] 수정된 Phase 2 실행
    await runUpdateWorkload(client, numDocs);

  } catch (err) {
    console.error('[GENERATOR] Error:', err);
  } finally {
    await client.close();
    console.log('[GENERATOR] Disconnected from MongoDB.');
  }
}

main();

