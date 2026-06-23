//! 公開型と内部識別子。

/// メッセージの一意な識別子。enqueue 順に単調増加で採番される。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MsgId(pub(crate) u64);

/// 配送試行のたびに更新される世代トークン。stale な ack/nack を弾くために使う。
///
/// メッセージが再配送されると世代が変わるので、古いリースを持つ worker の
/// 遅延 ack は拒否される(timeout と ack の競合対策)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct Generation(pub(crate) u64);

/// [`crate::Queue::deliver`] が返す配送リース。`ack`/`nack` に渡して結果を確定する。
#[derive(Debug, Clone)]
pub struct Lease<T> {
    pub id: MsgId,
    pub payload: T,
    pub(crate) generation: Generation,
}

/// `ack`/`nack` の結果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// 受理された。
    Ok,
    /// リースが古い(再配送済み or 既に終端)ため拒否された。stale ack 拒否。
    Stale,
}

/// メッセージの最終的な行き先。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Terminal {
    /// 正常に ack された。
    Acked,
    /// 最大試行回数を超えて DLQ(dead letter queue)へ移った。
    Dead,
}

/// 各状態の件数。`pending + inflight + acked + dead` は投入総数に一致する。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Counts {
    pub pending: usize,
    pub inflight: usize,
    pub acked: usize,
    pub dead: usize,
}

impl Counts {
    /// これまで投入された(終端含む)メッセージ総数。
    pub fn total(&self) -> usize {
        self.pending + self.inflight + self.acked + self.dead
    }
}
