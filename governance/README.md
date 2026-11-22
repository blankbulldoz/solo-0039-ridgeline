# Node 标签数据治理清单（只读评估，不改库）

**结论先行**：去大小写可以合并，但必须限定在 `node` 一个字段；去域后缀必须拒绝；缺失 rack 与显式空 rack 暂不合并；`ridge-a` 在更换设备当天只能在变更窗口之外按物理设备映射，窗口内一律标为未决。所有结论都用引擎返回的 SeriesID 核验，没有一条是按展示名去重得到的。

- 数据库变更：**无**。未修改任何已有库、未执行迁移、未重写标签。
- 评估方式：把 seed 扩展成治理专用 fixture，导入**临时** bbolt 文件，通过公开 HTTP API 读回引擎实际生成的 SeriesID 与观测值。
- 证据文件：
  - `governance/fixtures/extended-seed.json`（扩展输入，含业务声明与冲突点）
  - `governance/evidence/series-verification.json`（引擎返回的原始证据）
  - `governance/handoff/mapping-manifest.json`（机器可读映射，接入人员直接消费）
  - `governance/handoff/mapping.csv`（同一映射的表格视图）
- 复现：
  ```sh
  go build -o /tmp/ridgeline ./cmd/ridgeline
  python3 -I governance/verify/verify_series.py --binary /tmp/ridgeline \
      --out governance/evidence/series-verification.json
  python3 -I governance/verify/build_handoff.py
  ```
  校验脚本在全部关键检查未通过时会以非零码退出。

## 1. 身份规则（为什么不能按展示名去重）

`internal/model/validate.go:39` 的 `SeriesID` 是整个标签 map 的 SHA-256：

```go
func SeriesID(labels Labels) string {
	b, _ := json.Marshal(labels)
	hash := sha256.Sum256(b)
	return hex.EncodeToString(hash[:])
}
```

由此有三个直接影响治理的事实：

1. **大小写、后缀、rack 是否存在都会改变 ID**。`ridge-a` 与 `RIDGE-A` 是两个 SeriesID；`rack:""` 与没有 `rack` 键也是两个 SeriesID。
2. **ID 是存储主键的一部分**。`internal/volume/keys.go:8` 的 `PointKey(series, timestamp)` 以 64 位十六进制 ID 作为前缀，`internal/engine/ingest.go:49` 写入时调用它；block 的 `SeriesID`、tombstone 的 `Series` 列表、rollup 结果里的序列 ID 也都引用它。**没有 relabel API**，改写 ID 会让已存的 head、block、删除区间全部失去引用。
3. **匹配器的语义比 ID 宽**。`internal/select/match.go:47` 是 `value := labels[p.matcher.Label]`，缺失标签取空串。所以 `rack=""` 会同时命中“显式空 rack”和“没有 rack 键”的序列——**这正是"按展示名去重"会踩的坑**：查询层面两者不可区分，存储层面却是两条序列。

因此本清单只做“逻辑映射建议”，不产生任何 ID 迁移。

## 2. 扩展系列清单（12 条，全部为引擎实测 ID）

夹具以 `examples/input.json` 的 `spindle` seed 为基线扩展，命名空间 `spindle_gov`，时间统一到 2026-11-18 UTC。seed 中已有的 4 条序列在引擎里重新算出的 ID 与 `examples/session.json` 记录完全一致（`seed_cross_check=true`），说明扩展没有改变基线身份。

| 键 | 指标 | node 展示名 | rack 状态 | 引擎 SeriesID |
| --- | --- | --- | --- | --- |
| S1a | pulse_total | `ridge-a` | 缺失 | `ae37407678ef71c5a33ebe50d20bef7a18161f25a7a244d2c842f41f0bf0dc87` |
| S1b | pulse_total | `RIDGE-A` | 缺失 | `7c8a040e5b92f306c40103a28f951cd22b88c2a8e0e6c1a20c3b37559af21356` |
| S1c | pulse_total | `ridge-a.lab` | 缺失 | `a00b37f4a2b2a5d8b5e99f8b8bedc947639e39edff70f2b0b1aaa2261d0d52e4` |
| S1d | pulse_total | `RIDGE-A.lab` | 缺失 | `d94174f200cd39b964e1ddaf0469423529b712669f40057fcf4c17882ac2866e` |
| S1e | pulse_total | `ridge-a.prod` | 缺失 | `fa85531517fbc221066b270d840ef64ea9b006a6629470ec6692f0a95e5ea418` |
| S1f | pulse_total | `RIDGE-A.prod` | 缺失 | `fb0e801ca35d15e6b8fb678a8e5a307a4c95f1fe31fca2e89e4a19089b3c1bfe` |
| S2 | pulse_total | `ridge-b` | 缺失 | `c9c72d2a25e46bc3425bd051c939c305b7e03e5fffc80d9b83c3815ca46cad69` |
| S3a | load_ratio | `ridge-a` | **显式空** | `39221d1b5192ba577bd99dc708b70f6d0ad8d49fe739499eb321c5761d975d5f` |
| S3b | load_ratio | `RIDGE-A` | **显式空** | `0fe0950ee24d4d1765f626ce91fcde862666fe40075c0797946af87bd95205b8` |
| S3c | load_ratio | `ridge-a` | 缺失 | `287614e883fda058677eb79ee313049e8df4a33a815322ed7bd482447d123f0d` |
| S3d | load_ratio | `RIDGE-A` | 缺失 | `5fa5a5528904232f552389ce95f0832df758c93b286a8a25c9131a61fba30587` |
| S4 | load_ratio | `ridge-b` | 缺失 | `0f8fa51d16cf44764e3ef8d5bec20a41dee666c396716d60014c4f6859710888` |

同时扩展了 `zone` 的取值：seed 里 `ridge-b` 已同时出现在 `zone=east`（pulse_total）和 `zone=west`（load_ratio），所以 **`zone` 不能当作设备身份字段**，本清单所有映射都不按 zone 合并或拆分。

## 3. 两项规范化分别会合并什么

以 SeriesID 为输入做静态分组（`governance/evidence/series-verification.json` 的 `display_name_dedup` 与 `baseline_groups`）：

| 规范化 | 原 12 条 → | 合并桶 | 冲突样本数（桶×时刻） |
| --- | --- | --- | --- |
| 不做任何处理（基线） | 12 | 12 个桶，全部 count=1 | 0 |
| 仅去大小写 | 12 → **7** | 5 个两两合并桶 | **1** |
| 仅去后缀（保留大小写） | 12 → 8 | 2 个 count=3 的桶 | **14** |
| 去大小写 + 去后缀 | 12 → **5** | 含 1 个 count=6 的桶 | **7**（单点最多 4 个值） |
| 仅折叠 rack 存在性 | 12 → 10 | 2 个两两合并桶 | 0 |
| 去大小写 + 折叠 rack | 12 → 6 | 1 个 count=4 的桶 | 1 |
| 三项全做 | 12 → 4 | 1 个 count=4 + 1 个 count=6 | 7 |

关键点：

- **去后缀的冲突与大小写无关**。只去后缀就产生 14 个冲突样本（2 个大小写桶 × 7 个时刻），每桶每时刻都是 3 个不同值——因为 `.lab`/`.prod` 的数值分布本来就不同（300/700 量级 vs 100 量级）。把大小写也去掉后两个桶并成一个，冲突样本降为 7 个，但同一时刻变成 4 个不同值。冲突样本数变少不代表更安全，只代表合并得更深。这证明“去后缀无效”不是大小写决策的副作用。
- **去大小写只产生 1 个冲突**，且落在更换设备的时间窗内（见 §4）。
- **rack 折叠本身不产生值冲突**（S3a/S3b 与 S3c/S3d 在 7 个时间点的值一致），但这**不能**当作“两者同源”的证据；业务表明确说空值来源未知。

## 4. 更换设备的时间约束

业务事实（fixture 中 B4/B5）：旧采集组在治理当日更换设备后继续使用 `ridge-a` 名称，日志只把更换时刻限定在 **2026-11-18 12:00–12:10 UTC** 这十分钟内，不能定位到秒，也不能说明某个样本由哪台物理设备产生。

因此 `ridge-a` / `RIDGE-A` 这一身份（大小写归并后的同一个设备逻辑名）必须拆成三段：

| 段 | 区间（半开） | 物理设备 | 结论强度 |
| --- | --- | --- | --- |
| 变更前 | `[2026-11-18T00:00Z, 12:00Z)` | `dev:ridge-a@legacy` | 仅在该时段成立 |
| 变更窗口 | `[12:00Z, 12:10Z)` | **未决**（`null`） | 不成立，禁止归因 |
| 变更后 | `[12:10Z, 2026-11-19T00:00Z)` | `dev:ridge-a@replacement` | 仅在该时段成立 |

而大小写等价本身（`ridge-a` ≡ `RIDGE-A`）是**全部时间成立**的，与上面的物理设备拆分是正交的两件事：

- “`ridge-a` 和 `RIDGE-A` 属于同一设备（B1）”→ **全时段成立**。
- “该名称在某时刻对应哪台物理硬件”→ **只在窗口外成立**。

`.lab` / `.prod` 的映射是**全时段成立**的标签身份（B2 只说两者互不相同），但它们与基名 `ridge-a` 的关系业务表没给（B6），所以不给出“同一设备”的结论。

## 5. 冲突样本

### C1 — 去大小写后同设备同时间不同值（窗口内）

| 时间 | SeriesID | 值 | 提交序列 |
| --- | --- | --- | --- |
| 2026-11-18T12:04:00Z | `7c8a040e…1356`（`RIDGE-A`） | 512 | 2 |
| 2026-11-18T12:04:00Z | `ae374076…dc87`（`ridge-a`） | 412 | 3 |

- 目前两条序列各自独立，冲突**不存在于库里**；是“去大小写”这一映射把它们变成同一时刻两个值。
- 序列号 3 > 2，但它们属于不同序列，`sequence` 不是同一时间线的先后，**不能**据此选 412。
- 下游若按 `lower(node)` 聚合，该分钟会被平均成 462（`exact` 查询则为 412），两者都不是可验证的设备值。
- 归属：**未决**，交接入人员按提交日志与设备序列号裁决。

### C2 — 去后缀把不同设备并成一条（窗口内外都有）

去后缀（无论是否同时去大小写）会把 `pulse_total` 的 6 条序列按大小写分成两个 count=3 的桶；每个桶在**全部 7 个观测时刻**都出现 3 个不同值：

| 时间 | 一个桶内的 3 个值（基名 / .lab / .prod） |
| --- | --- |
| 11:40Z | 100 / 300 / 700 |
| 11:50Z | 110 / 310 / 710 |
| 12:00Z | 120 / 320 / 720 |
| 12:04Z | 412 / 330 / 730（`RIDGE-A` 桶为 512 / 330 / 730） |
| 12:09Z | 140 / 340 / 740 |
| 12:20Z | 150 / 350 / 750 |
| 12:30Z | 160 / 360 / 760 |

如果把大小写也去掉，两个桶再并成 count=6 的桶，12:04Z 会出现 4 个不同值（412、512、330、730）。冲突样本数：单独去后缀 14 个（2 桶 × 7 时刻），再去大小写 7 个（1 桶 × 7 时刻）。

这不是数据错误，而是 **B2 明确说 `.lab` 与 `.prod` 是不同设备**：去后缀等于合并不同设备的曲线。结论是拒绝该规范化，不需要“值对账”。

### C3 — 同一序列同时间被覆盖（窗口内）

`ae374076…dc87` 在 12:04Z 先后提交 410、412，引擎按“最后提交值生效”只保留 412。410 只存在于提交日志。这是**存储层的同时间值冲突**，任何身份映射都无法恢复，需提交日志裁决。

## 6. 推荐映射（保留身份含义）

推荐策略逐项给出结论与成立范围：

| 规范化 | 决定 | 成立范围 | 依据 |
| --- | --- | --- | --- |
| 去大小写（仅 `node` 值） | **采用** | `ridge-a`↔`RIDGE-A`：全部时间；`.lab`/`.prod` 的大小写对：按规则推断，需业务确认 | B1；U5 |
| 去域后缀 | **拒绝** | — | B2、B6；C2 |
| 折叠 rack 缺失/显式空 | **暂不合并**，保持两条 ID | — | B3；匹配器层面不可区分，见 §8 |
| 用 `zone` 参与身份 | **禁止** | — | seed 中 `ridge-b` 跨 east/west |
| 改写既有 SeriesID | **禁止** | — | ID 是存储键，无 relabel 通道 |

### 6.1 节点级对应表

| 展示名 | 归一化 | 业务设备 | 与 `ridge-a` 关系 | 成立范围 |
| --- | --- | --- | --- | --- |
| `ridge-a` / `RIDGE-A` | `ridge-a` | `ridge-a` | 同一设备（B1） | 大小写等价：**全部时间**；物理设备：**仅窗口外**（见 §4） |
| `ridge-a.lab` / `RIDGE-A.lab` | `ridge-a.lab` | `ridge-a.lab` | 与 `.prod` 不同设备；与基名关系未知 | 标签身份：**全部时间**；与基名的设备关系：**未决** |
| `ridge-a.prod` / `RIDGE-A.prod` | `ridge-a.prod` | `ridge-a.prod` | 与 `.lab` 不同设备；与基名关系未知 | 标签身份：**全部时间**；与基名的设备关系：**未决** |
| `ridge-b` | `ridge-b` | `ridge-b` | 未在业务表覆盖 | 标签身份：全部时间 |

### 6.2 系列级映射（含时间分段）

完整表见 `governance/handoff/mapping.csv`。摘要：

- `S1a`、`S1b`、`S3a`、`S3b`、`S3c`、`S3d`（`node` 为 `ridge-a` 大小写两型）：
  - 窗口前 → `dev:ridge-a@legacy`（确认）
  - 窗口内 → `UNDECIDED`（交接）
  - 窗口后 → `dev:ridge-a@replacement`（确认）
  - 大小写伙伴关系 → 全部时间
- `S1c`/`S1d` → `dev:ridge-a.lab`；`S1e`/`S1f` → `dev:ridge-a.prod`；全时段按标签身份成立。
- `S2`/`S4` → `dev:ridge-b`；全时段。

## 7. 无法判断项（交接清单）

| ID | 议题 | 为什么不能判断 | 责任方 | 所需证据 |
| --- | --- | --- | --- | --- |
| U1 | 精确切换时刻 | 日志只给出十分钟区间 | 接入组 + 旧采集组运维 | 窗口内带设备序列号/agent ID 的记录 |
| U2 | 基名 `ridge-a` 与 `.lab`/`.prod` 的关系 | B2 只说明 `.lab≠.prod` | 业务方 | 资产台账中展示名到设备序列号的映射 |
| U3 | rack 空值来源 | B3 明确来源未知 | 采集方 | 生产者配置：rack 是省略还是显式空 |
| U4 | 12:04Z 的同时间值冲突（C1、C3） | 身份映射无法裁决数值 | 接入组 + 两代设备采集方 | 带生产者标识的原始提交日志 |
| U5 | `.lab`/`.prod` 大小写对是否同设备 | B1 只点名 `ridge-a`/`RIDGE-A` | 业务方 | 明确声明或资产台账匹配 |
| U6 | `zone` 是否可作为属性 | seed 中 `ridge-b` 跨 east/west | 采集方 | 权威的站点/区域分配表 |
| U7 | 治理日之外的有效性 | 更换日志只覆盖一天 | 旧采集组运维 | 覆盖完整保留期的变更历史 |

### 待生产核验的假设

- **A1**：fixture 中每个 node 的两种大小写在变更窗口两侧都存在。若生产里 `RIDGE-A` 只出现在变更前、`ridge-a` 只出现在变更后，那么“去大小写”会把旧设备和新设备跨切换点并成一台。此时 B1 只在各自时段成立。核验方法：用 `node=~"(?i)ridge-a"` 查每个 SeriesID 的首末时间。
- **A2**：窗口外同一设备的大小写两型数值一致（fixture 中唯一的差异就是窗口内的 C1）。核验方法：按时间点比较两型的值。

## 8. 接入人员操作手册

**冻结要求**：不改标签、不改 SeriesID、不跑去后缀/折叠迁移、对账期间源库只读或只用副本。

**查询包**（全部半开区间，已用引擎实测，结果见 `series-verification.json` 的 `handoff_queries`）：

```json
{"start": 1794960000000, "end": 1795003200000}   // 窗口前：12 系列 × 2 点 = 24 点，可直接映射
{"start": 1795003200000, "end": 1795003800000}   // 窗口内：12 系列 × 3 点 = 36 点，必须挂起
{"start": 1795003800000, "end": 1795046400000}   // 窗口后：12 系列 × 2 点 = 24 点，可直接映射
{"matchers":[{"label":"node","op":"=~","value":"(?i)ridge-a"}]}   // 6 条序列，证明两型并存
{"matchers":[{"label":"node","op":"=","value":"ridge-a"}]}        // 3 条，仅精确小写
{"matchers":[{"label":"rack","op":"=","value":""}]}               // 12 条：显式空与缺失都被命中
{"start": 1795003440000, "end": 1795003440001,
 "matchers":[{"label":"__name__","op":"=","value":"pulse_total"},
             {"label":"node","op":"=~","value":"(?i)ridge-a"}]}    // 冲突复现：412 与 512
```

**必须注意的两个查询陷阱**（已实测）：

1. **`rack=""` 无法区分存在性**：返回 12 条（显式空的 2 条 + 缺失的 10 条）。要区分，只能按 SeriesID 过滤，不能靠 matcher。
2. **`node` 精确匹配只返回一种大小写**：要覆盖两型必须用 `=~"(?i)…"`；实现会把表达式包成 `^(?:…)$`（`internal/select/match.go:32`），因此上例的 `(?i)ridge-a` 是整串锚定的，不会误抓带后缀的名字。

**处理步骤**：

1. 先确认 U1–U7，答复按 SeriesID 记录，不按展示名。
2. 拉取窗口前、后两段，可立即按 §6 映射。
3. 拉取窗口内 36 个点，单独存放，**不得**标成 legacy 或 replacement。
4. 对窗口内每个点附生产者证据（序列号/agent ID），再拆分。
5. C1/C3 的数值由提交日志裁决；不得平均、不得默认取较大或较新值。
6. U1、U4 关闭后，才对外发布“设备级”序列；存储键继续用原 SeriesID。
7. 生产者标签规则变更后重跑 `verify_series.py`；除 `exact_queries_isolate_single_identity`
   外应全部为 true，且 `only_rack_rows_are_presence_ambiguous` 必须保持 true（否则说明出现了新的、无法用 matcher 隔离的身份对）。

## 9. 核验结果摘要

`governance/evidence/series-verification.json` 中的关键检查（全部来自运行中的引擎，不是脚本自证）：

| 检查 | 结果 |
| --- | --- |
| 12 条声明的标签集各自对应唯一引擎身份 | true |
| 重新计算的 SHA-256 与引擎 `id` 一致 | true |
| 全量 range 的序列集合 = catalog 集合 | true |
| 基线不做规范化时 12 条互不合并 | true |
| 同一序列同一时间只保留一个存储值（410→412） | true |
| 三个时间段覆盖全部 12 条序列 | true |
| 三段窗口互不重叠且首尾相接 | true |
| 除 4 条 rack 相关行外，精确标签查询能隔离单条身份 | true（4 条 rack 行无法隔离） |
| 只有 rack 相关行存在“存在性不可区分” | true |
| 冲突复现返回 412 与 512 | true |
| seed 的 4 条基线 ID 与 `examples/session.json` 一致 | true |

`exact_queries_isolate_single_identity` 为 false 是**被有意保留的真实结论**：4 条 rack 行无法用任何 matcher 互斥选中，必须由客户端按 SeriesID 过滤。这一点已单独记录为 `rack_presence_needs_series_id_filter`。
