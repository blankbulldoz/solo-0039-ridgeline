# 只读空间整理、恢复写入与保留边界推进 —— 值班手册

适用版本：本仓库当前 HEAD（`52625b0` 基线）在 Go 1.25 下构建的
`cmd/ridgeline`。文中所有行为都用 `examples/handoff_drill.py` 与
`examples/handoff_capture.py` 在临时库上验证过，数字来自
`examples/handoff_drill.py` 的一次完整运行（93 项断言，见第 9 节）。

本文回答四件事：

1. 推进保留边界（retention floor）之前必须留下哪些证据，证据对应哪一个来源状态。
2. 哪些情况下只能保留旧报表、不能为它背书。
3. 一次推进成功、一次整理因扫描预算失败时，值班凭什么继续、原样重试或停止。
4. 块与 head 的变化、文件大小、物化新鲜度、原始区间可重算性为什么是四件事，各自怎么核实。

同库的另一个空间（下文称 `live`）在整场操作中持续采集，**不因演练停写、不停进程、不整库恢复**。

---

## 0. 先记住的六条硬事实

这些是当前实现的确定性行为，不是建议。来源见括号内的代码位置。

| # | 事实 | 来源 |
|---|---|---|
| 1 | 只读空间允许读和**物理维护**：`seal`、`compact` 在只读下照常执行并推进序列；`points`、`deletions`、`retention`、rollup 物化被 409 拒绝。 | `internal/engine/seal.go:32`、`internal/engine/compact.go:38`、`internal/engine/rollup.go:20` |
| 2 | 命名空间序列（`last_sequence`）与全局序列（`/image` 头、`range.sequence`、`result.sequence`）是两个数。物化 rollup 只推进全局序列，不动命名空间序列。 | `internal/engine/rollup.go:28-33`、`internal/volume/db.go:73` |
| 3 | `stale` 只表示 `source_sequence != namespace.last_sequence`。它是**序列不相等**，不是内容校验，也不是正确性证明。 | `internal/engine/rollup.go:52` |
| 4 | 推进保留 = 只改 floor + 整空间重写（compact），**不重写任何 rollup**；旧 rollup 原样保留并变 stale。 | `internal/engine/compact.go:51-69` |
| 5 | 维护与查询共用 `max_scan`。扫描预算是**解码点数**，超出返回 422 `capacity`，整个事务回滚。 | `internal/engine/scan.go:58,88`、`internal/engine/compact.go:13` |
| 6 | bbolt 复用空闲页，**不承诺文件变小**；删除/整理后文件大小可能完全不变。 | `PROJECT_SPEC.md:47-49`、`README.md:89-94` |

---

## 1. 术语与四个“看起来一样”的观察

| 观察 | 怎么取 | 它证明什么 | 它**不**证明什么 |
|---|---|---|---|
| 物理位置 | `GET /spaces/{s}/blocks` 的块 id/区间/count；`POST /explain` 的 `head_records`、`source_point_hint` | 点在 head 还是块里、块被替换没有 | 逻辑数据没变；可重算 |
| 文件大小 | `ls -l $RIDGE_DB`；`GET /image` 的 `Content-Length` | 磁盘占用与镜像字节数 | 数据是否被回收；整理是否成功 |
| 物化新鲜度 | `GET /spaces/{s}/rollups/{r}` 的 `stale`、`source_sequence`、`result.sequence` | 相对**该空间**序列是否落后 | 报表值是否正确；被清区间能否核对 |
| 原始可重算性 | `POST /range` + `POST /aggregate` 在目标区间能否返回与旧报表相同的 cell | 现在还能不能从原始点重算出这段 | 历史报表当时算得对不对 |

**为什么必须分开**：`seal`/`compact` 会改块 id、清 head、推进序列，从而把 rollup 标成 stale，
但逻辑点一个不少（演练中 `range` 前后逐点相等）；同时文件字节数 65536 → 65536 不变。
反过来，一次失败的 `compact` 四个观察全部不变，却也没整理成。用其中一个去推断另一个一定出错。
演练把四个维度分别落在 `evidence.json` 的 `four_facts` 键下，便于逐项核对。

第三条的实践含义：**`stale=true` 只说明“该空间自物化以来动过”**，维护、只读切换、一条无关的删除都会让它变 stale；
而在**本空间**内 `stale=false` 确实意味着自物化后没有任何写入/删除/维护。
即便如此它也不是内容校验：它不检验已存字节是否算对、查询区间是否覆盖你要核对的段，
也不检验这份记录是不是从旧版本/别的库恢复来的。内容是否可核对必须靠
`range`/`aggregate` 重算对照（第 5 节），不能只看 `stale`。

---

## 2. 边界推进前：保留哪些证据、保留到哪个来源状态

### 2.1 目标来源状态

“来源状态”指**同一个已提交状态**下的一整套读数，必须同时满足：

- 命名空间的 `last_sequence` 在采集前后一致（本空间无人写、无维护穿插）；
- `/image` 的 `X-Ridge-Sequence` 与各读取响应里的 `sequence` **相等**（全局序列同一个）；
- `floor` 仍是推进前的值，目标区间尚未低于它。

`examples/handoff_capture.py` 用这三条做一致性判定，任一不满足就把 bundle 标成
`consistent:false`、退出码 1，要求重采或改用镜像。两个容易踩的点：

- 采集必须在**旧报表已经存在**之后（只读空间无法再物化）；采集期间任何物化
  （包括别的空间）都会推高全局序列，使 `consistent:false`。
- `live` 的写入只推高全局序列、不动本空间 `last_sequence`，所以它同样会让
  `consistent:false` —— 这正是它的用途：告诉你“这份证据不是同一个已提交状态”。
  正确反应是缩窄采集窗口重采，而不是停 `live`。

### 2.2 必须保留的证据清单（逐项可核对）

| 证据 | 取自 | 留存形式 |
|---|---|---|
| 来源状态头 | `GET /spaces/{s}` + `/image` 头 | 序列、floor、watermark、read_only、镜像 SHA-256/字节数 |
| 镜像本体 | `GET /image` | 原样字节 + 头里的 SHA-256 一起存；这是同一状态的根证据 |
| 将被清区间的原始点 | `POST /range`，区间 = 拟推进边界以下的 rollup 覆盖段 | 完整 JSON + SHA-256，且 `truncated` 必须为 false |
| 同区间的重算结果 | `POST /aggregate`（rollup 原查询，区间裁到被清段） | 完整 JSON + SHA-256 |
| 旧报表在被清段的 cell | `GET rollups/{r}` 里按 `start ∈ [区间)` 取子集 | 子集 + SHA-256，并给出整表 SHA-256 |
| 整表重算对照 | `POST /aggregate`（rollup 原查询，不裁区间） | 判断是否整表一致 |
| 物化血缘 | rollup 的 `source_sequence`、`result.sequence`、`query`、`stale` | 物化记录（去掉 `stale`）的 SHA-256 |
| 物理与删除侧证 | `GET blocks`、`GET deletions`、`POST explain` | 块 id/区间/count、tombstone id、head_records |
| 文件事实 | `ls -l`、镜像 `Content-Length` | 字节数 |

`handoff_capture.py` 把上述打包为 `capture.json` + `image.ridge.db`，`source_state`
与 `examples/handoff_drill.py` 的 `purged_segment.source_state` 主要字段同名同义
（capture 额外给出 `export_global_sequences` 与采集前后命名空间序列；它拿不到数据库文件字节数，
以镜像字节数代替 `database_file_bytes`），便于交叉核对。

被清区间怎么定：把拟推进后的 floor 记为 `F'`，rollup 查询区间记为 `[S,E)`，
被清段 = `[max(S, 低于F'的最小整块边界), min(E, F'))`。最稳妥的做法是**让 `F'` 落在窗口边界上**；
若落在窗口内部，该窗口仍会存在但值会变化（见 4.4），此时应把整个受影响窗口也纳入被清段的证据。

### 2.3 从哪里取

- **同库另一空间正在采集**：不要停服务、不要用 `SIGTERM` 去换“干净窗口”。用
  `handoff_capture.py` 对着运行中的进程只读采集；一致性靠序列判定，不靠停写。
  采集期间 `live` 有写入导致 `consistent:false` 时，缩窄采集窗口重采；
  **不要**用停 `live` 来换一致性，也不要事后用旧镜像去“补齐”。
- **绝对不能**为了拿到“一致镜像”把 `live` 停掉后整库恢复旧镜像：`/image` 含**所有空间**，
  旧镜像会覆盖 `live` 后续已确认的写入（第 8 节有实测）。

### 2.4 事后接班人如何核对被清段

原始点被清后，`POST /range`、`POST /aggregate` 在旧区间返回空。
接班人核对只依赖证据包，不依赖当前库：

1. 校验 `capture.json` 里 `raw_sha256`、`aggregate_sha256`、`stored_cells_sha256` 三者与内嵌 JSON 一致
   （演练里分别位于 `capture.json` 顶层与 `purged_segment` 下，字段同名）；
2. 校验 `stored_cells` 与 `aggregate.cells` 在被清区间上逐 cell 相等（`avg` 还要比 `count`）；
3. 校验 `stored_cells` 的 `start` 全部落在声明的被清区间内；
4. 校验 `source_state.rollup_stale=false` 时必有 `source_sequence == namespace_last_sequence`；
5. 校验 `purged_segment`（或 `capture.json`）与 `image.ridge.db` 来自同一状态：
   镜像头 `X-Ridge-Sequence` 等于 `source_state.global_sequence`；
6. 需要重放时，用随包的 `image.ridge.db` 在**隔离路径**起一个实例，重跑同一查询，
   对照 `purged_segment.raw_sha256`。不要把该镜像指到在用库路径。

`handoff_drill.py --verify evidence.json` 自动完成 1–5 与 `four_facts`、成功/失败结论的完整性检查（当前 15 项）。

---

## 3. 立刻要做的五步（只读空间）

以下每步都基于同一个运行中的进程；`space` 为待整理空间，`live` 为同库采集空间。

### 步骤 1 —— 冻结写入意图，不动 maintenance 能力

```
PUT /ridge/v1/spaces/{space}/mode   {"read_only": true}
```

- 期望：返回 `read_only:true`，`last_sequence` +1（切模式本身会推进序列）。
- 之后 `POST points` / `deletions` / `retention` / rollup 物化 → 409 `conflict`。
- `seal`、`compact` **仍然可执行**：这正是“只读整理”的入口。
- 不要去动 `live` 的 mode。

### 步骤 2 —— 采集证据（在动手前）

```
python3 examples/handoff_capture.py \
  --base http://127.0.0.1:8399 --space {space} --rollup {rollup} \
  --purge-start {S} --purge-end {F'} --output var/handoff-capture
```

- 退出码 0：一致，且整表与重算一致 → 可继续。
- 退出码 2：一致，但结论是 `drift` / `partial` / `expired` / `empty-interval` / `drift-purged-ok`
  → 按第 5 节决定“只留旧报表、不背书”。
- 退出码 1：不一致（有并发写入/维护）→ 重新采集；**不要**用停机换一致性。

采集本身不算 maintenance，不需要批准；它只读。

判定 `consistent` 的三条（`handoff_capture.py` 的 `checks.capture_consistent`）：
命名空间序列采集前后相等、镜像与所有读取的全局 `sequence` 只有一个值、
镜像序列 ≥ 命名空间序列。三者的含义见 2.1。

> 只读空间不能重采“新证据”，只能采“旧报表的对照证据”：物化被 409 拒绝，
> 所以 `handoff_capture.py --rollup` 指向的必须是**之前已物化**的报表。
> 若在进只读前没有物化过，就没有旧报表可核对，第 5 节的矩阵不适用，
> 只能记录“无报表可背书”，并在恢复写入后全新物化。

### 步骤 3 —— 物理整理（只读下进行）

1. `POST /spaces/{space}/seal` → 回执 `sequence`、`blocks`、`points`。
   - head 清空（`explain.head_records` → 0），块数 = 有数据的序列数；
   - **不改 floor**，**不动 rollup**，但会推进命名空间序列 → 旧 rollup 变 stale。
2. `POST /spaces/{space}/compact` → 回执 `sequence`、`blocks`、`points`。
   - 块 id 全部替换为新序列前缀；tombstone 被合并消费，`GET deletions` 变空；
   - 逻辑 `range` 前后必须逐点相等（这是整理的验收条件）；
   - 文件大小通常不变。

> `seal` 空 head 也会推进序列并产出 0 块，这是正常的；它会让 rollup 变 stale。

验收点：整理后重新跑一次 `handoff_capture.py`（或至少要一次 `range` 对照与 `explain`），
确认 `source_point_hint` / 可见点数未变、tombstone 已清理。**整理后旧 rollup 已 stale，
这不代表它错了**——它只是落后于维护产生的序列。

### 步骤 4 —— 批准后恢复写入

```
PUT /ridge/v1/spaces/{space}/mode   {"read_only": false}
```

- 只能在这一步之后写入；恢复写入前必须已经在步骤 2/3 留下证据。
- 恢复写入会推进序列；如果此前旧 rollup 还是 fresh，这一步也会让它 stale。
- `live` 全程不参与。

### 步骤 5 —— 推进保留边界

```
POST /ridge/v1/spaces/{space}/retention   {"now": N}
```

- 新 floor = `max(旧 floor, N - retention_ms)`（`internal/policy/admit.go:32-44`），**不可回落**。
- 期望回执：`floor` = 批准值、`sequence`、`points`（保留点数）。
- 成功后若用**同一被清区间**重采，会得到 `expired`——这是预期的，
  此时应以**步骤 2 的束**为准，而不是新束。
- 之后旧 rollup 仍是 stale 且内容不变；如需新报表，**用新名字重新物化**
  （旧名字重物化会覆盖历史记录，见第 5 节补充规则）。

---

## 4. 四个维度分别怎么核实

### 4.1 块与 head 的变化

- 取：`GET /blocks`（id、`start`、`end`、`count`、`checksum`、`bytes`）+ `POST /explain`（`head_records`、`source_point_hint`）。
- 判读：
  - `head_records` 从 22 → 0、块从 0 → 2：seal 成功；
  - 块 id 的 20 位前缀从 `…0010` 变成 `…0011`：compact 重新发布；
  - 块 id 不变而 head 减小：只有 seal 发生。
- 演练实测（archive 空间）：
  - `before_tidy`：head=22，blocks=0，`source_point_hint=0`；
  - `after_seal`：head=0，blocks=2（各 11 点），`source_point_hint=22`；
  - `after_tidy`：head=0，blocks=2，id 前缀 `00000000000000000011`；
  - `after_advance`：blocks=2（各 8 点），`source_point_hint=16`。
- 不要用块数变化推断“点变多/变少”；count 之和才是物理点数，且 tombstone 可能仍使其不可见。

### 4.2 文件大小

- 取：`ls -l $RIDGE_DB` 与 `/image` 的 `Content-Length`（两者不同：镜像是快照字节，文件含空闲页）。
- 判读：**只用于容量规划**，不能作为整理或删除是否生效的证据。
- 演练实测：archive 全程 65536 字节不变（成功整理 + 推进边界）；tight 空间 131072 字节在失败前后不变。
- 若磁盘告警，正确做法是换库/扩盘/离线重建，而不是反复 `compact` 期待变小。

### 4.3 物化新鲜度

- 取：`GET /spaces/{s}/rollups/{r}` → `stale`、`source_sequence`、`result.sequence`。
- 判读：
  - `source_sequence == namespace.last_sequence` ⇒ `stale=false`，否则 `true`；
  - `result.sequence` 是**物化那一刻的全局序列**，通常 ≥ `source_sequence`；
  - 只读切换、seal、compact、retention、任何写入都会让该空间序列前进 → 旧 rollup 立即 stale；
  - 另一空间（`live`）写入**不会**让本空间 rollup stale（演练断言过）。
- 演练实测：物化时 `source_sequence=6`、`result.sequence=6`；
  整理阶段（切只读、seal）后命名空间序列 6→10，stale=true；
  compact 后 10→11，仍 stale；推进后 13，仍 stale，且 `result.cells` 逐字节未变。

### 4.4 原始区间能否重算

- 取：对目标区间分别 `POST /range`（看原始点是否还在、是否 truncated）与
  `POST /aggregate`（用 rollup 原 query，把区间裁到目标段），与旧报表子集对照。
- 判读：
  - 区间完全在 floor 之上 → 能重算，逐 cell 应与旧报表相同；
  - 区间跨 floor → 只能重算 floor 以上的部分，旧报表的该段**不可验证**；
  - 区间完全在 floor 之下 → 返回空，永久不可重算；
  - 即使区间仍在，若期间有修正/删除，重算结果也会与旧报表不同——这时旧报表只是“历史快照”。

**窗口内部分过期**是最容易误判的一种（drill 的 `boundary_partial`）：
floor=1250 时 `[1000,2000)` 窗口仍然出现，但 cell 从
`avg=1.416666…, count=3` 变成 `avg=1.625, count=2`，`[0,1000)` 窗口消失。
所以**“cell 还在”不等于“值还可核对”**。

---

## 5. 旧报表能留到什么程度：背书矩阵

`handoff_capture.py` 的 `verdict` 字段与下表一一对应（退出码 2 即“可保留、需谨慎背书”）。

| verdict | 条件 | 可以做什么 | 不可以做什么 |
|---|---|---|---|
| `verified` | 一致；整表 = 重算；被清段 = 重算（**不要求** `stale=false`，维护造成的 stale 不算内容问题） | 在变更单中写明“旧报表在被清段经原始点核对通过”，并附哈希 | 不要把它当作 floor 推进**之后**的当前值；之后要新报表 |
| `drift-purged-ok` | 一致；被清段 = 重算；但整表其他窗口 ≠ 重算 | 只背书被清段；整表标注“历史记录，含未核对窗口” | 不得整体背书旧报表 |
| `drift` | 一致；被清段的原始点与旧报表不符 | 仅作为历史记录保留（存档、审计留痕） | 不得声称被清段被核对；不得对外引用其数值 |
| `partial` | 一致；floor 切进被清段，原始只剩一部分且与旧报表不符 | 保留旧报表；在交接单写明“该段已部分不可重算” | 不得说“已核对”；不得用剩余原始点重算后冒充整段 |
| `expired` | 一致；被清段已无任何原始点 | 旧报表是该段**唯一**记录，可按历史档案保留 | 不得称其“已核对”；不得据此后补任何修正 |
| `empty-interval` | 一致；旧报表在该段本来无 cell，原始也无点 | 记录“该段为空”，无需背书 | 不要把空段写成“已验证有数据” |
| `inconsistent` | 采集窗口内有并发写/维护，序列不一致 | 丢弃该束，重采 | 不得使用其中任何数值作为证据 |

补充规则：

- **物化之后、维护之前**若有人写入（哪怕一条无关的修正），旧报表已与当时原始不一致，
  此时只能按 `drift` 处理；不要因为“stale 是维护造成的”就认为内容没变——必须真去重算比对。
  反过来，维护本身也会让 `stale=true` 却内容一致（演练 `after_seal` 就是：`stale=true`，
  整表与重算相同，判为 `verified`）。**`stale` 两个方向都不能当内容结论用。**
- **旧报表被再次 `PUT` 同名重物化**会覆盖历史（`rollups` 桶按键覆盖）。要保留历史就必须保持旧名字只读，
  新报表用新名字（drill 用 `handover` → `handover-post`）。
- **不要用只读空间物化新报表**：会 409。新报表必须在恢复写入后创建（第 7 节第 6 步）。
- 只读整理期间**可以**执行 `seal`/`compact`；它们不改 floor，但会推进序列，所以旧报表的 `stale`
  会先于 floor 推进变 true。这是预期的，不代表旧报表内容出错。

---

## 6. 两次演练的结论：继续 / 原样重试 / 停止

### 6.1 第一次：推进成功（archive）

条件：`max_scan=100`，整理后 2 个块共 22 点，目标 floor=1000（窗口边界），区间 `[-2000,1000)` 被清 6 点。

结果：

- `retention` 回执 `floor=1000`、`points=16`、`sequence` 前进；
- 旧 rollup 的 `result.cells` **逐字节未变**（22 个 cell），`stale=true`；
- 新名字 `handover-post` 物化得 16 个 cell，被清段**静默缺席**；
- 被清段证据包三份哈希自洽，`--verify` 通过。

值班据此**继续**的依据（缺一不可）：

1. `retention` 回执的 `floor` 等于批准值，且 `sequence`/`points` 合理；
2. 推进后 `range` 在 floor 以上逐点与推进前相同（演练断言）；
3. 推进前的证据包已落盘并通过 `--verify`（raw / aggregate / stored_cells 三哈希 + 子集一致）；
4. 旧 rollup 内容未变（可以直接比 `result.cells` 的 SHA-256），且新报表换了名字；
5. `live` 仍在正常写入，且其 `last_sequence` 单调前进。

### 6.2 第二次：整理因扫描预算失败（tight）

条件：`max_scan=20`，head 30 点先 `seal`（成功，30 点进 1 块，seal 不花预算），
然后 `compact` 尝试解码该 30 点块 → 422 `capacity`。

结果（演练断言）：

- `compact` 首次失败与**原样重试**都返回 422 `capacity`；
- 命名空间 `last_sequence`、`floor`、块清单、`head_records` 在两次失败前后**完全相等**；
- 随后 `retention`（now=20，floor 目标 15）同样 422，floor 与序列均未动；
- 文件 131072 字节不变。

**失败本身是安全的**：事务整体回滚，没有半成品。依据这一点，值班可以**继续**（换手段）而不是恐慌。

判定规则：

| 情形 | 处置 |
|---|---|
| 422 `capacity`，且前后状态快照相等 | **停止当前操作**；不要原样重试（确定性失败，演练已证两次相同）。 |
| 想把预算“撑过去” | **禁止**：调大 `now` 会让 floor 越过整块，块被整块跳过并删除（见 6.3）。 |
| 需要一个真正可行的路径 | 需要变更批准：要么调大该空间 `max_scan`（配置只能建新空间时设定，当前 API 无 patch），要么按块/区间分步整理（当前没有“按系列 compact”的 API，需新版本或离线工具）。在批准前，保持只读、保留证据包、让 `live` 继续。 |
| 失败发生在 `retention`（floor 试图推进） | **停止**：floor 未变，旧报表仍可对照；不要换 `now` 重试。 |
| 失败后有人改了 `max_scan` 或代码 | 重跑 `handoff_capture.py` 与新状态对齐后再决策；不要沿用旧证据。 |

**为什么不能原样重试**：`scan()` 按块 `Count` 预扣预算（`internal/engine/scan.go:58`），
输入只增不减，重试的输入与原次完全一致，结果必然相同；白白占用维护窗口。

### 6.3 反例：把 `now` 调大“让预算通过”

演练 `anti_pattern`：同样 30 点、`max_scan=20`、批准 floor=15。
把 `now` 设成 1000 → floor=995，整块（end=30）低于 floor 被**整块跳过**，
预算检查不触发，`retention` 成功返回，30 点全部丢失（`range` 为空）。

回执看起来和正常成功一样（`floor`、`sequence` 都在），但这是**不可逆的数据删除**，
不是“边界推进成功”。值班必须核对：**`retention` 回执的 `floor` 是否等于批准值**，
以及失败前后 `floor` 是否只应前进到批准位置。floor 超出批准值 = 事故，立即停止写入并上报。

同为“跳过整块”，`budget_anatomy` 三种情形要分清：

| 情形 | 预算表现 | 结果 |
|---|---|---|
| head 未 seal，floor 很高 | head 点**不受 floor 影响**全部计入 | 422，安全回滚 |
| 一个块跨 floor | 整块解码（30 > 20） | 422，安全回滚 |
| 一个块完全低于 floor | 整块跳过，0 解码 | 成功，但整块数据被清 |

---

## 7. 恢复写入与推进边界的顺序（含批准）

1. （已完成）只读 + 整理 + 证据包。
2. 变更批准必须写明：目标 floor、被清区间、证据包路径与 SHA-256、旧报表背书等级（第 5 节）。
3. `PUT mode {read_only:false}`。
4. 在**批准窗口内**立刻 `POST retention {now:N}`；不要先写入新数据再推进（会改变可见集，证据包失配）。
5. 推进后验收：`GET space`（floor）、`range` floor 以上逐点对照、旧 rollup 的 `result.cells` SHA-256 未变、`live` 正常。
6. 需要新报表时用**新名字**物化；旧名字保持不动。

若第 4 步失败（422/其他）：保持旧 rollup 与证据包，按 6.2 处置；此时空间已可写，
但要立即评估是否回退到只读，避免写入使证据包进一步偏离。

---

## 8. 为什么不能整库恢复，以及正确做法

`/image` 包含**全部空间**，且是某一时刻的全局快照。演练（`restore_drill`）实测：

- 取镜像时全局序列 53，`live.last_sequence=8`；
- 取镜像后 `live` 又确认了一条写入 → `live.last_sequence=54`
  （命名空间序列记录的是该次写入的**全局序列**，所以会直接跳到 54，而不是 9）；
- 用该镜像在隔离路径启动，恢复出的 `live.last_sequence=8`，**不含**取镜像后的那条写入；
- 与此同时，原进程中的 `live` 完好保留该写入，未被影响。

结论：

- 用旧镜像覆盖在用库 = 回滚 `live` 的已确认写入，违反“不能为演练停它”。
- 正确姿势：**只在隔离路径启动**（`RIDGE_DB=/new/path/restore.db`），比对 `floor`、序列、原始点、stale，
  需要什么就只从隔离实例导出什么；用完后关闭隔离实例。
- 若确实必须从镜像恢复 `archive` 单空间：当前 API 没有单空间导出/导入，
  只能在隔离实例上读出来，再通过 API 重新写入目标空间；这期间 `live` 照常运行。
- 恢复前先确认没有第二个进程打开目标文件（bbolt 文件锁，第二个进程会等 2 秒后超时退出）。

---

## 9. 可复现材料

### 9.1 演练脚本

```
go build ./...
python3 examples/handoff_drill.py --workdir /tmp/ridge-handoff \
    --operator <值班> --approval <变更单号>
python3 examples/handoff_drill.py --verify /tmp/ridge-handoff/evidence.json
```

- 只用临时库、临时端口、合成数据；不接触生产。
- `--workdir` 会保留 `source.db`、`restore-old.db`、`ridgeline` 与 `evidence.json`，整目录可归档给接班人。
- 一次运行 93 项断言；同一源码下重启运行结果逐字节一致（演练中比较过两次运行的
  `purged_segment`、`rollup_assessment`、`four_facts`、块 id、镜像 SHA-256）。
- 失败时非零退出，不会留下“看起来成功”的半份证据。

`evidence.json` 关键键：

| 键 | 内容 |
|---|---|
| `build` | `go version` 与 `git HEAD` |
| `operations` | 全部请求/响应/线路字节 SHA-256（含 422 与 409） |
| `captures` | 每阶段快照：序列、floor、head、块、tombstone、文件、rollup |
| `purged_segment` | 被清段原始点/重算/旧 cell + 三哈希 + `source_state`（13 字段） |
| `pre_advance_image` | 与 `purged_segment` 同状态的镜像序列/哈希/字节数 |
| `rollup_assessment` | 旧报表与重算的对照与结论 |
| `four_facts` | 四个维度各自的证据与说明 |
| `capacity_failure` | tight 空间两次失败的状态快照与处置结论 |
| `budget_anatomy` | head / 跨块 / 整块跳过三种预算情形 |
| `boundary_partial` | 窗口内部分过期的 cell 变化 |
| `drift_rollup` | 维护前就已不一致的报表 |
| `restore_drill` | 旧镜像缺 `live` 后续写入的实证 |
| `conclusions` | 汇总（成功推进、失败处置、各条警告） |

### 9.2 现场采集（只读）

```
python3 examples/handoff_capture.py --base http://127.0.0.1:8399 \
    --space {space} --rollup {rollup} \
    --purge-start {S} --purge-end {F'} --output var/handoff-capture/{date}
```

产物：`capture.json` + `image.ridge.db`。`capture.json` 含
`source_state`（含 `export_global_sequences`、前后命名空间序列）、`purged_segment`、
`checks`、`verdict`、`endorsement`。退出码：0 可背书；2 可保留需谨慎；1 不一致须重采。

### 9.3 本手册数字的原始出处

全部来自 `python3 examples/handoff_drill.py --workdir /tmp/ridge-handoff` 的一次运行，
对应 `git HEAD 52625b0`、`go1.25.3`。若 HEAD 变化，先重跑演练再引用数字。

---

## 10. 交接清单（打印可用）

- [ ] 空间名、rollup 名、目标 floor、被清区间 `[S,F')`
- [ ] 证据包路径 + `capture.json` SHA-256 + `image.ridge.db` SHA-256 + 镜像序列
- [ ] `capture.json` 的 `verdict` 与 `endorsement` 原文
- [ ] 旧报表 `stored_cells_sha256` 与被清段 `stored_cells` 子集
- [ ] 整理前后：head_records、块 id 列表、tombstone id 列表
- [ ] 文件字节数（仅容量参考，不作为整理证据）
- [ ] 旧 rollup：`source_sequence`、`result.sequence`、`stale`（推进后应仍为旧值）
- [ ] 新报表名（若已物化）及其 `source_sequence`
- [ ] `live` 在采集窗口内的写入情况（是否导致 `consistent:false`）
- [ ] 失败记录：错误码、前后状态快照、处置决定（停止/批准后另法）
- [ ] 恢复演练：只在隔离路径，镜像序列、`live` 缺失写入的序列范围

---

## 11. 快速排错

| 现象 | 最可能原因 | 处理 |
|---|---|---|
| `compact`/`retention` 422 `capacity` | `max_scan` 小于需解码点数 | 停止；按 6.2 走批准流程；不要调 `now` |
| `retention` 成功但 `range` 全空 | floor 被 `now` 推过整块 | 视为事故；核对回执 floor 是否等于批准值；上报 |
| 旧 rollup 在整理后 stale | 维护推进了命名空间序列 | 正常；不代表报表错，按第 5 节核对内容 |
| `handoff_capture.py` 退出 1 | 采集窗口内有写入/维护 | 缩窄窗口重采；不要停机 |
| 恢复实例打不开库 | 另一进程已打开 | 换隔离路径或停**隔离**实例；不要动在用实例 |
| 文件大小不变 | bbolt 复用空闲页 | 正常；容量问题另行处理 |
| `seal` 回执 0 块 | head 已空 | 正常；但仍会推进序列并让 rollup stale |
