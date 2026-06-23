//! ready キューの優先度順序。priority 降順 → seq 昇順(FIFO タイブレーク)。

use std::cmp::Ordering;

use crate::types::MsgId;

/// ready ヒープのエントリ。`BinaryHeap` が pop する「最大」が次に配送すべきもの。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Slot {
    pub priority: i64,
    pub seq: u64,
    pub id: MsgId,
}

impl Ord for Slot {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap は最大要素を pop する。priority が大きいほど「大」、
        // 同 priority なら seq が小さいほど「大」(先に出す)。
        self.priority
            .cmp(&other.priority)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}

impl PartialOrd for Slot {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BinaryHeap;

    // priority 降順 → seq 昇順で pop されること(配送順の核)。
    #[test]
    fn pops_in_priority_then_fifo_order() {
        let mut h = BinaryHeap::new();
        h.push(Slot { priority: 0, seq: 0, id: MsgId(0) });
        h.push(Slot { priority: 5, seq: 1, id: MsgId(1) });
        h.push(Slot { priority: 5, seq: 2, id: MsgId(2) });
        let order: Vec<u64> = std::iter::from_fn(|| h.pop().map(|s| s.id.0)).collect();
        // priority 5 が先(同値は seq 小さい id=1 が先)、最後に priority 0。
        assert_eq!(order, vec![1, 2, 0]);
    }
}
