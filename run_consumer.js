/**
 * Change Stream 소비자 (v2)
 * - 치명적인 '타임아웃' 및 '측정 오염' 오류 수정
 * - 'insert' 이벤트는 무시하고, 'update' 이벤트만 측정
 * - 'update'가 처음 감지될 때 타이머 시작
 */

const { MongoClient } = require('mongodb');
const yargs = require('yargs/yargs');
const { hideBin } = require('yargs/helpers');

// [!!! 중요 !!!] 이 URI의 <...> 부분을 당신의 GCP '내부 IP'로 수정하십시오!
// 소비자는 Primary(mongo-p)에만 직접 연결해도 됩니다. (단순화를 위해)
// (물론 전체 rs URI를 써도 동일하게 작동합니다.)
const MONGO_URI = 'mongodb://10.142.0.2:27017/?replicaSet=rs0';

const DB_NAME = 'testdb';
const COLLECTION_NAME = 'testcoll';

// 커맨드라인 인자 파싱
const argv = yargs(hideBin(process.argv))
  .option('fullDocument', {
    alias: 'f',
    type: 'string',
    description: "Change Stream fullDocument 옵션 ('default' 또는 'updateLookup')",
    demandOption: true,
  })
  .option('batchSize', {
    alias: 'b',
    type: 'number',
    description: '애플리케이션 처리 배치 크기',
    demandOption: true,
  })
  .argv;

// M2 (지연 시간) 측정을 위한 평균 계산기
class LatencyTracker {
  constructor() {
    this.totalLatency = 0n; // bigint (nanoseconds)
    this.batchCount = 0;
  }
  addBatch(batchStartTime) {
    const latencyNs = process.hrtime.bigint() - batchStartTime;
    this.totalLatency += latencyNs;
    this.batchCount++;
  }
  getAverageMs() {
    if (this.batchCount === 0) return 0;
    const avgNs = this.totalLatency / BigInt(this.batchCount);
    return Number(avgNs) / 1_000_000; // ms
  }
}

// 메인 실행 함수
async function watchCollection() {
  const client = new MongoClient(MONGO_URI);
  const { fullDocument, batchSize } = argv;

  // v2 로직 변수
  let updatesStarted = false;       // Update 단계가 시작되었는지
  let globalStartTime = null;       // M1 측정을 위한 전체 타이머
  let processedEventCount = 0;    // M1 측정을 위한 카운터
  const latencyTracker = new LatencyTracker(); // M2 측정기
  
  let batch = [];
  let shutdownTimer = null;         // Update 시작 후 비활성 감지 타이머
  let seedingTimeout = null;        // Seeding(Phase 1)을 기다리는 최대 타이머

  // [v2 수정] Seeding이 15분 이상 걸리면 오류로 간주하고 종료
  seedingTimeout = setTimeout(() => {
    console.error('[CONSUMER] FATAL: No events received for 15 minutes. Assuming generator failed.');
    client.close();
    process.exit(1);
  }, 900000); // 15분

  // [v2 수정] Update 단계 시작 후, 30초간 새 이벤트 없으면 종료
  function resetShutdownTimer() {
    if (shutdownTimer) clearTimeout(shutdownTimer);
    shutdownTimer = setTimeout(async () => {
      console.log(`[CONSUMER] No new 'update' events received for 30 seconds. Finalizing...`);
      // 남은 배치 처리
      if (batch.length > 0) {
        await processBatch(batch);
      }
      // 최종 결과 출력
      printFinalResults();
      await client.close();
      console.log('[CONSUMER] Disconnected from MongoDB.');
    }, 30000); // 30초
  }

  // 배치 처리 로직 (시뮬레이션)
  async function processBatch(currentBatch) {
    const batchStartTime = process.hrtime.bigint();
    
    // --- 'default' 모드 시뮬레이션 ---
    // 'default' 모드일 때만, DB에 추가 조회를 수행한다고 가정
    // (실제로는 `find` 쿼리를 날려야 함)
    if (fullDocument === 'default') {
      // (시뮬레이션: 네트워크/DB 조회 지연시간을 흉내 내기 위해 잠시 대기)
      // await new Promise(resolve => setTimeout(resolve, 1)); // 1ms 대기
    }
    // 'updateLookup' 모드는 이 작업이 필요 없음
    // --- 시뮬레이션 끝 ---

    processedEventCount += currentBatch.length;
    latencyTracker.addBatch(batchStartTime); // M2 (지연 시간) 기록
  }

  // 최종 결과 출력
  function printFinalResults() {
    const totalTimeNs = process.hrtime.bigint() - globalStartTime;
    const totalTimeSec = Number(totalTimeNs) / 1_000_000_000;
    const throughput = (totalTimeSec > 0) ? (processedEventCount / totalTimeSec) : 0;
    const avgLatencyMs = latencyTracker.getAverageMs();

    console.log('\n[CONSUMER] --- FINAL RESULTS ---');
    console.log(`Scenario: fullDocument=${fullDocument}, batchSize=${batchSize}`);
    console.log(`Total Events Processed: ${processedEventCount}`);
    console.log(`Total Processing Time: ${totalTimeSec.toFixed(2)} sec`);
    console.log(`M1 - Throughput: ${throughput.toFixed(2)} ops/sec`);
    console.log(`M2 - Avg. Batch Latency: ${avgLatencyMs.toFixed(2)} ms`);
    console.log('--- [CONSUMER] END ---');
  }


  try {
    await client.connect();
    console.log('[CONSUMER] Connected to MongoDB. Waiting for events...');
    const db = client.db(DB_NAME);
    const collection = db.collection(COLLECTION_NAME);

    const changeStream = collection.watch([], {
      fullDocument: fullDocument,
      maxAwaitTimeMS: 1000, // 1초마다 응답 (배치가 안 찼어도)
    });

    // Change Stream 이벤트 리스너
    for await (const change of changeStream) {
      
      // [v2 수정] Phase 1 (insert, drop)은 무시하고 타이머만 리셋
      if (change.operationType === 'insert' || change.operationType === 'drop') {
        clearTimeout(seedingTimeout); // Seeding이 진행 중이니 15분 타이머 리셋
        seedingTimeout = setTimeout(() => {
           console.error('[CONSUMER] FATAL: Seeding stopped for 15 minutes.');
           client.close(); process.exit(1);
        }, 900000);
        continue; // 측정하지 않고 무시
      }

      // [v2 수정] Phase 2 (update) 처리
      if (change.operationType === 'update') {
        
        // [v2 수정] 첫 번째 'update' 이벤트를 감지한 순간!
        if (!updatesStarted) {
          updatesStarted = true;
          clearTimeout(seedingTimeout); // Seeding 타이머 영구 제거
          globalStartTime = process.hrtime.bigint(); // [!!!] M1 타이머 지금 시작
          console.log(`[CONSUMER] --- UPDATE PHASE DETECTED ---`);
          console.log(`[CONSUMER] Starting metrics collection...`);
        }
        
        // [v2 수정] Update 시작 후에만 30초 종료 타이머 작동
        resetShutdownTimer();
        batch.push(change);

        if (batch.length >= batchSize) {
          await processBatch(batch);
          batch = []; // 배치 초기화
        }
      }
    }

  } catch (err) {
    console.error('[CONSUMER] Error:', err);
  } finally {
    // 예기치 않은 종료 시에도 client.close() 보장
    if (client) {
      await client.close();
    }
  }
}

watchCollection();

