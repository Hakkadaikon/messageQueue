//! エンドツーエンド: producer → worker プール → 完了 を実際のスレッドで動かす。
//!
//! 個々の遷移は `integration.rs` が網羅する。ここでは「実際の使われ方」全体が
//! 期待どおり振る舞うか(全ジョブ完遂・優先度尊重・クラッシュからの自動回復・DLQ)
//! をシナリオとして検証する。

use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use reliable_queue::{Outcome, Queue, Terminal};

// producer が投入したジョブを worker プールが全件処理しきる。
#[test]
fn producer_worker_pool_drains_all_jobs() {
    const JOBS: usize = 1000;
    const WORKERS: usize = 4;

    let q = Arc::new(Queue::with_timeout(5, Duration::from_secs(60)));
    let processed = Arc::new(Mutex::new(HashSet::new()));

    // producer: 別スレッドで投入。
    let producer = {
        let q = Arc::clone(&q);
        thread::spawn(move || {
            for i in 0..JOBS {
                q.enqueue(i, (i % 10) as i64);
            }
        })
    };

    // worker プール。
    let workers: Vec<_> = (0..WORKERS)
        .map(|_| {
            let q = Arc::clone(&q);
            let processed = Arc::clone(&processed);
            thread::spawn(move || loop {
                match q.deliver() {
                    Some(lease) => {
                        // 「処理」: 重複なく記録。
                        if q.ack(&lease) == Outcome::Ok {
                            processed.lock().unwrap().insert(lease.payload);
                        }
                    }
                    None => {
                        let c = q.counts();
                        // producer が全件投入済み かつ 在庫ゼロなら終了。
                        if c.total() == JOBS && c.pending == 0 && c.inflight == 0 {
                            break;
                        }
                        thread::yield_now();
                    }
                }
            })
        })
        .collect();

    producer.join().unwrap();
    for w in workers {
        w.join().unwrap();
    }

    let done = processed.lock().unwrap();
    assert_eq!(done.len(), JOBS, "全ジョブが処理される");
    assert_eq!(q.counts().acked, JOBS);
    assert_eq!((0..JOBS).collect::<HashSet<_>>(), *done, "取りこぼし・重複なし");
}

// worker がクラッシュしても、別 worker がタイムアウト回収して最終的に全件完了する。
#[test]
fn crashed_worker_jobs_are_recovered() {
    const JOBS: usize = 50;

    // タイムアウト短め。回収を実時間で観測する。
    let q = Arc::new(Queue::with_timeout(5, Duration::from_millis(20)));
    for i in 0..JOBS {
        q.enqueue(i, 0);
    }

    let acked = Arc::new(AtomicUsize::new(0));

    // 落ちる worker: いくつか deliver して ack せず「落ちる」(リースを捨てる)。
    {
        let q = Arc::clone(&q);
        thread::spawn(move || {
            for _ in 0..10 {
                let _lease = q.deliver(); // ack せず破棄 = クラッシュ相当
            }
        })
        .join()
        .unwrap();
    }

    // sweeper: 期限切れを回収し続ける。
    let sweeper = {
        let q = Arc::clone(&q);
        let acked = Arc::clone(&acked);
        thread::spawn(move || {
            while acked.load(Ordering::Relaxed) < JOBS {
                q.tick_timeouts();
                thread::sleep(Duration::from_millis(5));
            }
        })
    };

    // 正常 worker: 残り(回収分含む)を処理しきる。
    let worker = {
        let q = Arc::clone(&q);
        let acked = Arc::clone(&acked);
        thread::spawn(move || {
            while acked.load(Ordering::Relaxed) < JOBS {
                if let Some(lease) = q.deliver() {
                    if q.ack(&lease) == Outcome::Ok {
                        acked.fetch_add(1, Ordering::Relaxed);
                    }
                } else {
                    thread::sleep(Duration::from_millis(5));
                }
            }
        })
    };

    worker.join().unwrap();
    sweeper.join().unwrap();

    // クラッシュで失われた 10 件も回収され、全件完了。
    assert_eq!(acked.load(Ordering::Relaxed), JOBS);
    assert_eq!(q.counts().acked, JOBS);
}

// 恒常的に失敗するジョブは DLQ へ、健全なジョブは完了する(混在シナリオ)。
#[test]
fn poison_jobs_go_to_dlq_others_succeed() {
    const MAX: u32 = 3;
    let q = Queue::with_timeout(MAX, Duration::from_secs(60));

    // 偶数=健全, 奇数=毒(常に nack される)。
    let mut ids = Vec::new();
    for i in 0..20 {
        ids.push((i, q.enqueue(i, 0)));
    }

    // worker: 偶数は ack、奇数は nack。在庫が尽きるまで。
    while let Some(lease) = q.deliver() {
        if lease.payload % 2 == 0 {
            assert_eq!(q.ack(&lease), Outcome::Ok);
        } else {
            q.nack(&lease);
        }
    }

    for (payload, id) in ids {
        let expected = if payload % 2 == 0 {
            Terminal::Acked
        } else {
            Terminal::Dead
        };
        assert_eq!(q.terminal_of(id), Some(expected), "job {payload}");
    }
    let c = q.counts();
    assert_eq!(c.acked, 10);
    assert_eq!(c.dead, 10);
    assert_eq!(c.pending + c.inflight, 0);
}

// 優先度の高いジョブは、同時に大量投入された低優先ジョブより先に処理される。
#[test]
fn high_priority_jobs_processed_first() {
    let q = Queue::with_timeout(5, Duration::from_secs(60));

    // 低優先を大量、高優先を少量。
    for i in 0..100 {
        q.enqueue(format!("low-{i}"), 0);
    }
    for i in 0..5 {
        q.enqueue(format!("HIGH-{i}"), 100);
    }

    // 最初の 5 件は必ず高優先(単一 worker で逐次取り出し)。
    let first_five: Vec<_> = (0..5)
        .map(|_| {
            let l = q.deliver().unwrap();
            let p = l.payload.clone();
            q.ack(&l);
            p
        })
        .collect();

    assert!(
        first_five.iter().all(|p| p.starts_with("HIGH-")),
        "高優先が先: {first_five:?}"
    );
}
