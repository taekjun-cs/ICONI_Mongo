/**
 * 워크로드 생성기 (v2)
 * - Phase 1: Seeding (초기 데이터 삽입)
 * - Phase 2: Update (변경 이벤트 생성)
 * - 로그를 명확하게 분리
 */

const { MongoClient } = require('mongodb');
const yargs = require('yargs/yargs');
const { hideBin } = require('yargs/helpers');

// [!!! 중요 !!!] 이 URI의 <...> 부분을 당신의 GCP '내부 IP'로 수정하십시오!
// 복제본 세트(rs0) 전체에 연결해야 합니다.
const MONGO_URI = 'mongodb://10.142.0.2:27017,10.142.0.3:27017,10.142.0.4:27017/?replicaSet=rs0';

const DB_NAME = 'testdb';
const COLLECTION_NAME = 'testcoll';

// 커맨드라인 인자 파싱
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

// docSize(KB)에 맞는 거대한 문자열 페이로드 생성
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

// 1. Phase 1: Seeding
async function seedData(client, numDocs, docSize) {
  console.log(`[GENERATOR] --- PHASE 1 START ---`);
  console.log(`[GENERATOR] Seeding ${numDocs} documents of ${docSize}KB each...`);
  const db = client.db(DB_NAME);
  const collection = db.collection(COLLECTION_NAME);

  try {
    // 컬렉션 초기화
    await collection.drop();
    console.log(`[GENERATOR] Dropped old collection.`);
  } catch (err) {
    if (err.codeName !== 'NamespaceNotFound') {
      throw err;
    }
    // 컬렉션이 없으면 그냥 진행
  }

  const payload = generatePayload(docSize);
  const docs = [];
  for (let i = 0; i < numDocs; i++) {
    docs.push({
      docId: i,
      payload: payload,
      version: 0,
    });
  }

  // insertMany는 대량 삽입에 효율적
  await collection.insertMany(docs);
  console.log(`[GENERATOR] --- PHASE 1 COMPLETE --- (${numDocs} docs inserted)`);
}

// 2. Phase 2: Update Workload
async function runUpdateWorkload(client, numDocs) {
  console.log(`[GENERATOR] --- PHASE 2 START ---`);
  console.log(`[GENERATOR] Starting update workload for ${numDocs} documents...`);
  const db = client.db(DB_NAME);
  const collection = db.collection(COLLECTION_NAME);

  const updatePromises = [];
  for (let i = 0; i < numDocs; i++) {
    // 개별 문서를 순차적으로 업데이트하여 Change Stream 이벤트 발생
    const promise = collection.updateOne(
      { docId: i },
      { $set: { version: 1 } }
    );
    updatePromises.push(promise);
  }

  // 모든 업데이트가 완료될 때까지 기다림
  await Promise.all(updatePromises);
  console.log(`[GENERATOR] --- PHASE 2 COMPLETE --- (${numDocs} docs updated)`);
}

// 메인 실행 함수
async function main() {
  const client = new MongoClient(MONGO_URI);
  try {
    await client.connect();
    console.log('[GENERATOR] Connected to MongoDB replica set.');

    const { numDocs, docSize } = argv;

    // Phase 1 실행
    await seedData(client, numDocs, docSize);

    // Phase 2 실행
    await runUpdateWorkload(client, numDocs);

  } catch (err) {
    console.error('[GENERATOR] Error:', err);
  } finally {
    await client.close();
    console.log('[GENERATOR] Disconnected from MongoDB.');
  }
}

main();

