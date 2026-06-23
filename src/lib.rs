//! 優先度付き信頼性インメモリ・メッセージキュー。
//!
//! producer が [`Queue::enqueue`]、worker が [`Queue::deliver`] で取り出し、
//! [`Queue::ack`] で完了する。配送は at-least-once、処理は冪等(同一メッセージの
//! ack 成功は高々1回)。worker が落ちても [`Queue::tick_timeouts`] が期限切れの
//! 配送を回収するので、ack も DLQ 行きもされていないメッセージは消えない。
//!
//! 配送順は優先度の高い順。同一優先度のメッセージは enqueue 順(FIFO)で配送される。
//!
//! ```
//! use reliable_queue::{Queue, Outcome};
//!
//! let q = Queue::new(3); // 最大試行回数 3
//! q.enqueue("low", 0);
//! let hi = q.enqueue("high", 10);
//!
//! // 優先度が高い "high" が先に配送される。
//! let lease = q.deliver().unwrap();
//! assert_eq!(lease.id, hi);
//! assert_eq!(lease.payload, "high");
//! assert_eq!(q.ack(&lease), Outcome::Ok);
//! ```

mod queue;
mod slot;
mod types;

pub use queue::Queue;
pub use types::{Counts, Lease, MsgId, Outcome, Terminal};
