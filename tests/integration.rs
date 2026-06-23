//! property / 並行 / 復旧の結合テスト。
//!
//! 外部の property test クレートは使わず、決定的な擬似乱数(線形合同)で
//! 多数のケースを網羅して不変条件を検査する。

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use reliable_queue::{Outcome, Queue, Terminal};

/// 決定的な擬似乱数(再現性のため)。
struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1);
        self.0 >> 16
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

// SAFE-5 (property): 任意の enqueue 列に対し、全件 deliver した順序は
// priority 降順・同値 seq 昇順の「あるべき順」と一致する。
#[test]
fn property_delivery_order_is_priority_then_fifo() {
    for seed in 0..200u64 {
        let mut rng = Lcg(seed.wrapping_mul(2654435761).wrapping_add(1));
        let q = Queue::new(100);
        let n = 1 + rng.below(12) as usize;
        // (seq, priority) を投入順に記録。
        let mut expected: Vec<(usize, i64)> = Vec::new();
        for seq in 0..n {
            let priority = rng.below(4) as i64; // 同値を多く出すため小集合
            q.enqueue(seq, priority);
            expected.push((seq, priority));
        }
        // あるべき順: priority 降順、同値は seq(=投入順)昇順。
        expected.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        let expected_payloads: Vec<usize> = expected.iter().map(|(seq, _)| *seq).collect();

        let mut got = Vec::new();
        while let Some(lease) = q.deliver() {
            got.push(lease.payload);
            assert_eq!(q.ack(&lease), Outcome::Ok);
        }
        assert_eq!(got, expected_payloads, "seed {seed}");
    }
}

// SAFE-4 (property): ランダムな操作列の後、4 状態の総和は投入総数に一致し、
// ack 済み msg は二度と pending/inflight に現れない(終端の単調性)。
#[test]
fn property_state_counts_consistent() {
    for seed in 0..200u64 {
        let mut rng = Lcg(seed.wrapping_mul(40503).wrapping_add(7));
        let q = Queue::with_timeout(3, Duration::from_millis(0));
        let mut enqueued = 0usize;
        let mut live_lease = None;

        for _ in 0..40 {
            match rng.below(6) {
                0 => {
                    q.enqueue(enqueued, rng.below(3) as i64);
                    enqueued += 1;
                }
                1 => live_lease = q.deliver().or(live_lease),
                2 => {
                    if let Some(l) = live_lease.take() {
                        q.ack(&l);
                    }
                }
                3 => {
                    if let Some(l) = live_lease.take() {
                        q.nack(&l);
                    }
                }
                4 => {
                    q.tick_timeouts();
                    live_lease = None; // 期限切れでリースは無効化されうる
                }
                _ => {
                    q.crash();
                    live_lease = None;
                }
            }
            // 不変条件: 総和 = 投入数。常に成り立つ。
            assert_eq!(q.counts().total(), enqueued, "seed {seed}");
        }
    }
}

// LIVE-1: 上限まで失敗させ続ければ、全メッセージはいつか終端(acked か dead)に至る。
#[test]
fn liveness_all_messages_terminate() {
    let q = Queue::with_timeout(3, Duration::from_millis(0));
    let mut ids = Vec::new();
    for i in 0..5 {
        ids.push(q.enqueue(i, i as i64));
    }
    // ひたすら nack し続ける(worker が全部失敗するワーストケース)。
    for _ in 0..100 {
        while let Some(l) = q.deliver() {
            let _ = q.nack(&l);
        }
        q.tick_timeouts();
        if ids.iter().all(|id| q.terminal_of(*id).is_some()) {
            break;
        }
    }
    for id in &ids {
        assert_eq!(q.terminal_of(*id), Some(Terminal::Dead));
    }
    assert!(q.deliver().is_none());
}

// 復旧 (SAFE-3): どの遷移の途中で crash しても、acked/dead 以外のメッセージは
// 消えず、回収後に必ず終端へ到達できる。
#[test]
fn recovery_no_loss_under_crash_at_any_point() {
    for crash_at in 0..8 {
        let q = Queue::with_timeout(5, Duration::from_millis(0));
        let ids: Vec<_> = (0..4).map(|i| q.enqueue(i, i as i64)).collect();

        // crash_at ステップだけ進めてから crash。
        for step in 0..crash_at {
            if step % 2 == 0 {
                let _ = q.deliver();
            } else {
                q.crash();
            }
        }
        q.crash();

        // crash 直後: acked/dead 以外は消えていない(総和保存)。
        assert_eq!(q.counts().total(), 4, "crash_at {crash_at}");
        assert_eq!(q.counts().inflight, 0, "crash clears inflight");

        // 回収後、全件 ack できる(消えていないことの強い証拠)。
        let mut acked = 0;
        while let Some(l) = q.deliver() {
            if q.ack(&l) == Outcome::Ok {
                acked += 1;
            }
        }
        assert_eq!(acked, 4, "all messages survive crash_at {crash_at}");
        for id in &ids {
            assert_eq!(q.terminal_of(*id), Some(Terminal::Acked));
        }
    }
}

// SAFE-1 / SAFE-2 (並行): 複数 worker が同時に deliver/ack しても、
// 各メッセージはちょうど1回だけ ack される(二重処理も消失もない)。
#[test]
fn concurrent_workers_ack_each_message_once() {
    const N: usize = 500;
    const WORKERS: usize = 8;

    let q = Arc::new(Queue::with_timeout(50, Duration::from_secs(60)));
    for i in 0..N {
        q.enqueue(i, (i % 5) as i64);
    }

    let ok_acks = Arc::new(AtomicUsize::new(0));
    // 各 msgId が ack された回数(Outcome::Ok を返した回数)を数える。
    let counts = Arc::new(
        (0..N)
            .map(|_| AtomicUsize::new(0))
            .collect::<Vec<_>>(),
    );
    let barrier = Arc::new(Barrier::new(WORKERS));

    let handles: Vec<_> = (0..WORKERS)
        .map(|_| {
            let q = Arc::clone(&q);
            let ok_acks = Arc::clone(&ok_acks);
            let counts = Arc::clone(&counts);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                loop {
                    match q.deliver() {
                        Some(lease) => {
                            if q.ack(&lease) == Outcome::Ok {
                                ok_acks.fetch_add(1, AtomicOrdering::Relaxed);
                                counts[lease.payload].fetch_add(1, AtomicOrdering::Relaxed);
                            }
                        }
                        None => {
                            // 他スレッドが処理中の可能性。少し待って再確認。
                            if q.counts().pending == 0 && q.counts().inflight == 0 {
                                break;
                            }
                            thread::yield_now();
                        }
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    // 全件ちょうど1回 ack。
    assert_eq!(ok_acks.load(AtomicOrdering::Relaxed), N);
    let mut seen = HashMap::new();
    for (i, c) in counts.iter().enumerate() {
        let v = c.load(AtomicOrdering::Relaxed);
        assert_eq!(v, 1, "msg {i} acked {v} times (expected exactly 1)");
        seen.insert(i, v);
    }
    assert_eq!(seen.len(), N);
    assert_eq!(q.counts().acked, N);
}
