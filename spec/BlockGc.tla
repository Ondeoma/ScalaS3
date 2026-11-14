---------------------------- MODULE BlockGc ----------------------------
(***************************************************************************)
(* ブロックの GC（マーク＆スイープ）と、重複排除・共有の競合のモデル。       *)
(*                                                                         *)
(*  - ブロックは内容で識別するので、同じ内容の PUT は既存のブロックを再利用  *)
(*    する（更新時刻だけ進める）。                                          *)
(*  - コピーやマルチパートの完了は、既存のブロックを新しいレコードで共有する。*)
(*  - GC はメタデータを 1 キーずつ読んで参照を集め（マーク）、その後で       *)
(*    「マーク開始時刻 − 猶予」より古く、参照も使用中でもないブロックを消す。 *)
(*                                                                         *)
(* 安全性：コミットされた参照が指すブロックは、必ずディスクにある。          *)
(* TouchOnShare / TouchOnCommit（共有するとき・コミットするときに更新時刻を *)
(* 進める）のどちらかを FALSE にすると、TLC が反例を見つける。               *)
(*                                                                         *)
(* 例（TouchOnCommit = FALSE）：猶予期間より長いアップロードが、GC のマーク  *)
(* の途中で、もう読み終えたキーにコミットすると、そのブロックは参照されて    *)
(* いないとみなされて消される。                                             *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Blocks, Keys, MaxTime, Grace, TouchOnShare, TouchOnCommit

NULL == "null"

VARIABLES
    now,        \* 時刻
    stored,     \* ディスクにあるブロック
    mtime,      \* ブロックの更新時刻
    refs,       \* キー -> 参照しているブロック（コミット済みのメタデータ）
    inflight,   \* 書き込み中のアップロードが保護しているブロック
    phase,      \* GC の段階: "idle" | "mark" | "sweep"
    markStart,  \* マークを始めた時刻
    scanned,    \* マークで読み終えたキー
    marked      \* マークで見つけた参照

vars == <<now, stored, mtime, refs, inflight, phase, markStart, scanned, marked>>

TypeOK ==
    /\ now \in 0..MaxTime
    /\ stored \subseteq Blocks
    /\ mtime \in [Blocks -> 0..MaxTime]
    /\ refs \in [Keys -> Blocks \cup {NULL}]
    /\ inflight \subseteq Blocks
    /\ phase \in {"idle", "mark", "sweep"}

Init ==
    /\ now = Grace + 1
    /\ stored = {}
    /\ mtime = [b \in Blocks |-> 0]
    /\ refs = [k \in Keys |-> NULL]
    /\ inflight = {}
    /\ phase = "idle"
    /\ markStart = 0
    /\ scanned = {}
    /\ marked = {}

Tick ==
    /\ now < MaxTime
    /\ now' = now + 1
    /\ UNCHANGED <<stored, mtime, refs, inflight, phase, markStart, scanned, marked>>

\* アップロードがブロックを書く（既にあれば更新時刻だけ進める）。書き込み中は保護する
PutBlock(b) ==
    /\ stored' = stored \cup {b}
    /\ mtime' = [mtime EXCEPT ![b] = now]
    /\ inflight' = inflight \cup {b}
    /\ UNCHANGED <<now, refs, phase, markStart, scanned, marked>>

\* メタデータをコミットし、保護を外す。TouchOnCommit なら、コミットの直前にブロックの更新時刻を進める
\* （実装では「更新時刻を進めてからコミットする」の 2 段だが、その間は猶予期間よりずっと短いのでまとめて扱う）
Commit(k, b) ==
    /\ b \in inflight
    /\ refs' = [refs EXCEPT ![k] = b]
    /\ inflight' = inflight \ {b}
    /\ mtime' = IF TouchOnCommit THEN [mtime EXCEPT ![b] = now] ELSE mtime
    /\ UNCHANGED <<now, stored, phase, markStart, scanned, marked>>

Delete(k) ==
    /\ refs[k] # NULL
    /\ refs' = [refs EXCEPT ![k] = NULL]
    /\ UNCHANGED <<now, stored, mtime, inflight, phase, markStart, scanned, marked>>

\* コピー：既存のブロックを別のキーで共有する（1 回のメタデータの書き込み）
Copy(src, dst) ==
    /\ src # dst
    /\ refs[src] # NULL
    /\ refs' = [refs EXCEPT ![dst] = refs[src]]
    /\ mtime' = IF TouchOnShare THEN [mtime EXCEPT ![refs[src]] = now] ELSE mtime
    /\ UNCHANGED <<now, stored, inflight, phase, markStart, scanned, marked>>

MarkBegin ==
    /\ phase = "idle"
    /\ phase' = "mark"
    /\ markStart' = now
    /\ scanned' = {}
    /\ marked' = {}
    /\ UNCHANGED <<now, stored, mtime, refs, inflight>>

\* メタデータを 1 キーずつ読む（読む時刻はキーごとに違う）
MarkStep(k) ==
    /\ phase = "mark"
    /\ k \notin scanned
    /\ scanned' = scanned \cup {k}
    /\ marked' = IF refs[k] = NULL THEN marked ELSE marked \cup {refs[k]}
    /\ phase' = IF scanned' = Keys THEN "sweep" ELSE "mark"
    /\ UNCHANGED <<now, stored, mtime, refs, inflight, markStart>>

Sweep(b) ==
    /\ phase = "sweep"
    /\ b \in stored
    /\ b \notin marked
    /\ b \notin inflight
    /\ mtime[b] + Grace < markStart
    /\ stored' = stored \ {b}
    /\ UNCHANGED <<now, mtime, refs, inflight, phase, markStart, scanned, marked>>

SweepEnd ==
    /\ phase = "sweep"
    /\ phase' = "idle"
    /\ UNCHANGED <<now, stored, mtime, refs, inflight, markStart, scanned, marked>>

Next ==
    \/ Tick
    \/ \E b \in Blocks : PutBlock(b) \/ Sweep(b)
    \/ \E k \in Keys, b \in Blocks : Commit(k, b)
    \/ \E k \in Keys : Delete(k) \/ MarkStep(k)
    \/ \E s, d \in Keys : Copy(s, d)
    \/ MarkBegin
    \/ SweepEnd

Spec == Init /\ [][Next]_vars

\* 安全性：コミットされた参照が指すブロックは、ディスクにある
NoDanglingRefs == \A k \in Keys : refs[k] # NULL => refs[k] \in stored
=============================================================================
