# reliable_queue

優先度付きの信頼性インメモリ・メッセージキュー(Rust)。

producer が `enqueue` し、複数の worker が `deliver` で取り出して処理、`ack` で完了する。
外部依存なし・単一プロセス・スレッド間で安全に共有できる。

## 特徴

- **優先度配送**: 優先度の高いメッセージから配送。同一優先度は enqueue 順(FIFO)。
- **at-least-once 配送**: 処理が完了(ack)するまでメッセージは失われない。
- **クラッシュ復旧**: worker が落ちても、可視性タイムアウトを過ぎた配送は自動で回収され、別の worker に再配送される。
- **冪等な完了**: 同一メッセージの ack 成功は高々1回。二重 ack や、再配送後の古いリースによる ack は拒否される。
- **DLQ(dead letter queue)**: 最大試行回数を超えたメッセージは dead に移り、再配送されない。

## 使い方

```rust
use reliable_queue::{Queue, Outcome, Terminal};

let q = Queue::new(3); // 最大試行回数 3

// 投入(payload, priority)。priority が大きいほど先に配送される。
q.enqueue("send-welcome-email", 0);
let urgent = q.enqueue("charge-card", 10);

// worker が取り出す。優先度が高い "charge-card" が先に出る。
let lease = q.deliver().unwrap();
assert_eq!(lease.id, urgent);

// 処理に成功したら ack。
assert_eq!(q.ack(&lease), Outcome::Ok);
```

### 失敗と再配送

```rust
use reliable_queue::{Queue, Outcome, Terminal};

let q = Queue::new(2); // 2 回失敗したら DLQ へ
let id = q.enqueue("flaky-job", 0);

let lease = q.deliver().unwrap();
q.nack(&lease);            // 1 回目失敗 → 再配送待ち
let lease = q.deliver().unwrap();
q.nack(&lease);            // 2 回目失敗 → DLQ

assert_eq!(q.terminal_of(id), Some(Terminal::Dead));
```

### タイムアウトとクラッシュからの回収

worker が `ack`/`nack` せずに落ちた配送は、可視性タイムアウトを過ぎると
`tick_timeouts` で回収される。定期的に呼び出すこと。

```rust
use reliable_queue::Queue;
use std::time::Duration;

let q = Queue::with_timeout(5, Duration::from_secs(30));
q.enqueue("job", 0);
let _lease = q.deliver().unwrap(); // この worker が落ちたとする

// 別のスレッドや定期処理から:
let reclaimed = q.tick_timeouts(); // 期限切れの配送を回収
assert_eq!(reclaimed, 1);          // job は再び配送可能になる
```

`crash()` は全ての配送中メッセージを即座に回収するヘルパー(テストや graceful な
worker 入れ替えに使える)。回収されたメッセージは消えず、再配送される。

## API

| メソッド | 説明 |
| --- | --- |
| `Queue::new(max_attempts)` | キューを作る(可視性タイムアウト既定 30 秒) |
| `Queue::with_timeout(max_attempts, timeout)` | タイムアウトも指定 |
| `enqueue(payload, priority) -> MsgId` | メッセージを投入 |
| `deliver() -> Option<Lease>` | 最優先のメッセージを取り出す |
| `ack(&lease) -> Outcome` | 完了。古いリースは `Outcome::Stale` |
| `nack(&lease) -> Outcome` | 失敗。再配送または上限超で DLQ |
| `tick_timeouts() -> usize` | 期限切れの配送を回収 |
| `crash() -> usize` | 全配送中メッセージを即時回収 |
| `terminal_of(id) -> Option<Terminal>` | 最終結果(`Acked`/`Dead`)を問い合わせ |
| `counts() -> Counts` | 各状態の件数 |

## 設計上の前提

- インメモリ。プロセスが落ちると全メッセージが失われる(永続化は対象外)。
- 単一プロセス内のスレッド並行。分散は対象外。

## テスト

```sh
cargo test
```
