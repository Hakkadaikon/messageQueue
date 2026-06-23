//! キュー本体。状態保持・各遷移(enqueue/deliver/ack/nack/timeout/crash)。

use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::slot::Slot;
use crate::types::{Counts, Generation, Lease, MsgId, Outcome, Terminal};

#[derive(Debug)]
struct Msg<T> {
    payload: T,
    priority: i64,
    seq: u64,
    generation: Generation,
    attempts: u32,
    state: State,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Pending,
    InFlight { deadline: Instant },
}

struct Inner<T> {
    msgs: HashMap<MsgId, Msg<T>>,
    ready: BinaryHeap<Slot>,
    terminal: HashMap<MsgId, Terminal>,
    /// ack 済み msgId。二重 ack を冪等に弾く。
    acked: HashSet<MsgId>,
    next_id: u64,
    next_seq: u64,
    next_generation: u64,
}

/// 優先度付き信頼性メッセージキュー。スレッド間で共有して使える(内部ロック)。
pub struct Queue<T> {
    inner: Mutex<Inner<T>>,
    max_attempts: u32,
    visibility_timeout: Duration,
}

impl<T: Clone> Queue<T> {
    /// 最大試行回数を指定してキューを作る。可視性タイムアウトは既定 30 秒。
    pub fn new(max_attempts: u32) -> Self {
        Self::with_timeout(max_attempts, Duration::from_secs(30))
    }

    /// 可視性タイムアウトも指定する。配送後この時間 ack されないと再配送対象になる。
    pub fn with_timeout(max_attempts: u32, visibility_timeout: Duration) -> Self {
        assert!(max_attempts >= 1, "max_attempts must be >= 1");
        Queue {
            inner: Mutex::new(Inner {
                msgs: HashMap::new(),
                ready: BinaryHeap::new(),
                terminal: HashMap::new(),
                acked: HashSet::new(),
                next_id: 0,
                next_seq: 0,
                next_generation: 0,
            }),
            max_attempts,
            visibility_timeout,
        }
    }

    /// メッセージを優先度付きで投入し、その ID を返す。
    pub fn enqueue(&self, payload: T, priority: i64) -> MsgId {
        let mut g = self.inner.lock().unwrap();
        let id = MsgId(g.next_id);
        let seq = g.next_seq;
        let generation = Generation(g.next_generation);
        g.next_id += 1;
        g.next_seq += 1;
        g.next_generation += 1;
        g.msgs.insert(
            id,
            Msg {
                payload,
                priority,
                seq,
                generation,
                attempts: 0,
                state: State::Pending,
            },
        );
        g.ready.push(Slot { priority, seq, id });
        id
    }

    /// pending の中で最も優先度が高い(同値なら最古の)メッセージを配送する。
    /// 配送可能なメッセージが無ければ `None`。
    pub fn deliver(&self) -> Option<Lease<T>> {
        let now = Instant::now();
        let mut g = self.inner.lock().unwrap();
        // ヒープには stale な Slot(再配送で世代が変わった・既に inflight/終端)が
        // 混じりうる。Pending かつ seq が一致する生きた Slot に当たるまで捨てる。
        while let Some(slot) = g.ready.pop() {
            match g.msgs.get_mut(&slot.id) {
                Some(msg) if matches!(msg.state, State::Pending) && msg.seq == slot.seq => {
                    msg.state = State::InFlight {
                        deadline: now + self.visibility_timeout,
                    };
                    return Some(Lease {
                        id: slot.id,
                        payload: msg.payload.clone(),
                        generation: msg.generation,
                    });
                }
                _ => continue, // stale slot、捨てる
            }
        }
        None
    }

    /// リースを ack して完了させる。世代が古ければ [`Outcome::Stale`]。
    pub fn ack(&self, lease: &Lease<T>) -> Outcome {
        let mut g = self.inner.lock().unwrap();
        match g.msgs.get(&lease.id) {
            Some(msg)
                if matches!(msg.state, State::InFlight { .. })
                    && msg.generation == lease.generation =>
            {
                g.msgs.remove(&lease.id);
                g.acked.insert(lease.id);
                g.terminal.insert(lease.id, Terminal::Acked);
                Outcome::Ok
            }
            _ => Outcome::Stale, // 再配送済み / 既に終端 / 二重 ack
        }
    }

    /// リースを nack して再配送(または上限到達で DLQ 行き)させる。
    pub fn nack(&self, lease: &Lease<T>) -> Outcome {
        let mut g = self.inner.lock().unwrap();
        match g.msgs.get(&lease.id) {
            Some(msg)
                if matches!(msg.state, State::InFlight { .. })
                    && msg.generation == lease.generation =>
            {
                self.fail(&mut g, lease.id);
                Outcome::Ok
            }
            _ => Outcome::Stale,
        }
    }

    /// 期限切れの配送を回収する。worker のクラッシュや応答喪失を救う独立機構。
    /// 回収した(再 pending 化 or DLQ 行きした)件数を返す。
    pub fn tick_timeouts(&self) -> usize {
        let now = Instant::now();
        let mut g = self.inner.lock().unwrap();
        let expired: Vec<MsgId> = g
            .msgs
            .iter()
            .filter_map(|(id, m)| match m.state {
                State::InFlight { deadline } if deadline <= now => Some(*id),
                _ => None,
            })
            .collect();
        for id in &expired {
            self.fail(&mut g, *id);
        }
        expired.len()
    }

    /// 失敗(nack / timeout)を処理する。attempts++ し、上限到達なら DLQ、
    /// 未満なら世代を更新して再 pending 化する。
    ///
    /// 上限判定は **pending に戻す前** に行う。これを怠ると上限超過リトライが
    /// 1回漏れる(設計検証で踏んだ反例)。retry で attempts はリセットしない
    /// (リセットすると収束しなくなる)。
    fn fail(&self, g: &mut Inner<T>, id: MsgId) {
        let msg = g.msgs.get_mut(&id).expect("fail on existing inflight msg");
        msg.attempts += 1;
        if msg.attempts >= self.max_attempts {
            g.msgs.remove(&id);
            g.terminal.insert(id, Terminal::Dead);
            return;
        }
        // 再配送: 世代を更新(古いリースの ack/nack を無効化)し ready へ戻す。
        let new_gen = Generation(g.next_generation);
        g.next_generation += 1;
        let (priority, seq);
        {
            let m = g.msgs.get_mut(&id).unwrap();
            m.generation = new_gen;
            m.state = State::Pending;
            priority = m.priority;
            seq = m.seq;
        }
        g.ready.push(Slot { priority, seq, id });
    }

    /// クラッシュ模倣: 全ての配送中メッセージを即時に期限切れ扱いで回収する。
    /// メッセージは消えない。inflight は再 pending 化 or 上限超で DLQ。
    /// recover を別途呼ぶ必要はない(回収すれば再配送可能になる)。
    pub fn crash(&self) -> usize {
        let mut g = self.inner.lock().unwrap();
        let inflight: Vec<MsgId> = g
            .msgs
            .iter()
            .filter_map(|(id, m)| matches!(m.state, State::InFlight { .. }).then_some(*id))
            .collect();
        for id in &inflight {
            self.fail(&mut g, *id);
        }
        inflight.len()
    }

    /// メッセージの最終結果を返す(終端していなければ `None`)。
    pub fn terminal_of(&self, id: MsgId) -> Option<Terminal> {
        self.inner.lock().unwrap().terminal.get(&id).copied()
    }

    /// 現在の各状態の件数 (pending, inflight, acked, dead) を返す。整合性検査用。
    pub fn counts(&self) -> Counts {
        let g = self.inner.lock().unwrap();
        let mut pending = 0;
        let mut inflight = 0;
        for m in g.msgs.values() {
            match m.state {
                State::Pending => pending += 1,
                State::InFlight { .. } => inflight += 1,
            }
        }
        let acked = g
            .terminal
            .values()
            .filter(|t| matches!(t, Terminal::Acked))
            .count();
        let dead = g
            .terminal
            .values()
            .filter(|t| matches!(t, Terminal::Dead))
            .count();
        Counts {
            pending,
            inflight,
            acked,
            dead,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // enqueue は pending・件数整合を作る。
    #[test]
    fn enqueue_makes_pending() {
        let q: Queue<&str> = Queue::new(3);
        q.enqueue("a", 0);
        let c = q.counts();
        assert_eq!((c.pending, c.inflight, c.acked, c.dead), (1, 0, 0, 0));
    }

    // deliver→ack で acked。
    #[test]
    fn deliver_then_ack() {
        let q = Queue::new(3);
        let id = q.enqueue("a", 0);
        let lease = q.deliver().unwrap();
        assert_eq!(lease.id, id);
        assert_eq!(q.ack(&lease), Outcome::Ok);
        assert_eq!(q.terminal_of(id), Some(Terminal::Acked));
        assert_eq!(q.counts().acked, 1);
    }

    // 空キューの deliver は None(空 ready の境界)。
    #[test]
    fn deliver_empty_is_none() {
        let q: Queue<&str> = Queue::new(3);
        assert!(q.deliver().is_none());
    }

    // SAFE-5: priority 高→低の順に配送される。
    #[test]
    fn deliver_respects_priority() {
        let q = Queue::new(3);
        q.enqueue("low", 0);
        q.enqueue("high", 10);
        q.enqueue("mid", 5);
        let order: Vec<_> = (0..3).map(|_| q.deliver().unwrap().payload).collect();
        assert_eq!(order, vec!["high", "mid", "low"]);
    }

    // SAFE-5: 同 priority は FIFO(seq 昇順)。
    #[test]
    fn deliver_fifo_within_priority() {
        let q = Queue::new(3);
        q.enqueue("first", 5);
        q.enqueue("second", 5);
        q.enqueue("third", 5);
        let order: Vec<_> = (0..3).map(|_| q.deliver().unwrap().payload).collect();
        assert_eq!(order, vec!["first", "second", "third"]);
    }

    // higher が pending の間 lower を配ってはならない(優先度逆転なし)。
    #[test]
    fn no_priority_inversion_with_interleaving() {
        let q = Queue::new(3);
        q.enqueue("p0", 0);
        let hi = q.enqueue("p9", 9);
        // 後から入れた高優先が先に出る。
        assert_eq!(q.deliver().unwrap().id, hi);
    }

    // SAFE-1: ack 済みは二度と deliver されない。
    #[test]
    fn acked_never_redelivered() {
        let q = Queue::new(3);
        q.enqueue("a", 0);
        let lease = q.deliver().unwrap();
        assert_eq!(q.ack(&lease), Outcome::Ok);
        assert!(q.deliver().is_none());
    }

    // SAFE-2: 二重 ack は冪等に弾かれる(stale)。
    #[test]
    fn double_ack_is_stale() {
        let q = Queue::new(3);
        q.enqueue("a", 0);
        let lease = q.deliver().unwrap();
        assert_eq!(q.ack(&lease), Outcome::Ok);
        assert_eq!(q.ack(&lease), Outcome::Stale);
        assert_eq!(q.counts().acked, 1);
    }

    // nack で attempts++ し再配送。
    #[test]
    fn nack_requeues() {
        let q = Queue::new(3);
        let id = q.enqueue("a", 0);
        let lease = q.deliver().unwrap();
        assert_eq!(q.nack(&lease), Outcome::Ok);
        assert_eq!(q.counts().pending, 1);
        // 再配送される。
        assert_eq!(q.deliver().unwrap().id, id);
    }

    // 上限到達で DLQ。max=2 なら nack 2回で dead。
    #[test]
    fn reaches_dlq_after_max_attempts() {
        let q = Queue::new(2);
        let id = q.enqueue("a", 0);
        let l1 = q.deliver().unwrap();
        assert_eq!(q.nack(&l1), Outcome::Ok); // attempts=1, 再 pending
        let l2 = q.deliver().unwrap();
        assert_eq!(q.nack(&l2), Outcome::Ok); // attempts=2 >= max, dead
        assert_eq!(q.terminal_of(id), Some(Terminal::Dead));
        assert!(q.deliver().is_none());
        assert_eq!(q.counts().dead, 1);
    }

    // stale ack 拒否: 再配送で世代が変わった古いリースの ack は拒否される。
    #[test]
    fn stale_ack_rejected_after_redelivery() {
        let q = Queue::new(5);
        q.enqueue("a", 0);
        let old = q.deliver().unwrap();
        assert_eq!(q.nack(&old), Outcome::Ok); // 世代が更新される
        let new = q.deliver().unwrap();
        // 古いリースでの ack は拒否。
        assert_eq!(q.ack(&old), Outcome::Stale);
        // 新しいリースは受理。
        assert_eq!(q.ack(&new), Outcome::Ok);
    }

    // crash は inflight を消さない(SAFE-3)。回収後 pending で再配送可能。
    #[test]
    fn crash_does_not_lose_inflight() {
        let q = Queue::new(5);
        let id = q.enqueue("a", 0);
        let _lease = q.deliver().unwrap();
        assert_eq!(q.counts().inflight, 1);
        let recovered = q.crash();
        assert_eq!(recovered, 1);
        let c = q.counts();
        // 消えていない: pending に戻った。
        assert_eq!((c.pending, c.inflight), (1, 0));
        assert_eq!(q.deliver().unwrap().id, id);
    }

    // crash 後の stale worker の ack は拒否される(timeout/crash と ack の競合)。
    #[test]
    fn stale_ack_rejected_after_crash() {
        let q = Queue::new(5);
        q.enqueue("a", 0);
        let lease = q.deliver().unwrap();
        q.crash();
        assert_eq!(q.ack(&lease), Outcome::Stale);
    }

    // tick_timeouts は期限切れの inflight だけを回収する。
    #[test]
    fn timeout_reclaims_expired() {
        let q = Queue::with_timeout(5, Duration::from_millis(0));
        let id = q.enqueue("a", 0);
        let _lease = q.deliver().unwrap();
        // timeout=0 なので即期限切れ。
        assert_eq!(q.tick_timeouts(), 1);
        assert_eq!(q.deliver().unwrap().id, id);
    }

    // 期限内なら回収しない。
    #[test]
    fn timeout_keeps_fresh_inflight() {
        let q = Queue::with_timeout(5, Duration::from_secs(60));
        q.enqueue("a", 0);
        let _lease = q.deliver().unwrap();
        assert_eq!(q.tick_timeouts(), 0);
        assert_eq!(q.counts().inflight, 1);
    }
}
