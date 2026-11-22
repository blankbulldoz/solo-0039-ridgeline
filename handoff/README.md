# Ridgeline 同源备份分叉交接包

本包基于 `examples/input.json` 的 9 条合成观测，通过真实 HTTP API 构造了
「同一份 Ridgeline 备份恢复出多个分叉、同名 rollup 的 source_sequence 相同」
的现场，并附一份只改物理块布局、逻辑观测不变的对照组。所有结论都可由关系
数据独立复算，不依赖本说明。

## 1. 场景拓扑

```
base-seed.db（共享备份，namespace 最后序列 = 3）
   │  同一份镜像字节恢复到三个独立临时实例
   ├─ branch-a.db  观测分叉：t=500 同时间戳修正为 8.0
   ├─ branch-b.db  观测分叉：t=500 同时间戳修正为 9.0
   └─ branch-c.db  仅物理分叉：不写任何点，seal + compact 重排块
```

序列事实（两套计数器，勿混用）：

| 镜像 | namespace `last_sequence` | rollup `source_sequence` | 镜像头全局 sequence |
| --- | --- | --- | --- |
| base-seed.db | 3 | （尚未物化 rollup） | 3 |
| branch-a.db | **4** | **4**（pulse-avg / pulse-sum / pulse-caliber 全部） | 7 |
| branch-b.db | **4** | **4**（同名三个 rollup） | 7 |
| branch-c.db | 5 | 5 | 8 |

要点：rollup 物化只推进数据库全局 sequence，不推进 namespace 的
`last_sequence`（见 `internal/engine/rollup.go` 的注释与实现），所以
`source_sequence` 被钉在修正写入的那一档（4）。branch-a 与 branch-b 各自
独立从同一备份重放，各自得到 4——**跨镜像相同的 source_sequence 只表示
「本库内第 4 个变更之后」，不是全局内容身份**。

## 2. 三组 rollup 与两条拒绝规则

每个分支都物化同名报表，查询定义存于 `rollup_fact.query_json`：

- `pulse-avg`：三分支查询完全相同（avg, step 1000, origin 0, 按 zone 分组）。
  分叉体现在值上：窗口 `[0,1000)` 的 east 均值，branch-a = **7.5**、
  branch-b = **8.0**、branch-c = **4.5**（与 base-seed 一致）。
- `pulse-sum`：同样查询口径，仅聚合函数换成 sum，同样出现同序列不同值。
- `pulse-caliber`：branch-a/branch-c 用宽口径 `[-5000,10000)`，branch-b 用
  窄口径 `[-2000,2000)`。这是「查询口径不一致」的实证——即使值碰巧相同，
  口径不同也不能归为同一来源。

归并规则（已固化为视图 `rollup_pair_verdict.verdict`）：

1. `query_json` 不同 **或** 存储的单元格值指纹不同 →
   `REJECT_COMMON_SOURCE`，无论 source_sequence 是否相等；
2. 逻辑指纹相同、仅物理指纹不同 → `SAME_OBSERVATIONS_LAYOUT_ONLY`；
3. 两者都相同 → `IDENTICAL_EVIDENCE`。

镜像级对照在 `image_pair_verdict`：base-seed ↔ branch-c 被判定为
`SAME_OBSERVATIONS_LAYOUT_ONLY`（9 条采样逐点一致，但 4 个块的 ID/校验和
全部改变），任何逻辑相等的镜像对都不会被误判为 `OBSERVATIONS_DIVERGE`。

## 3. 关系数据怎么用

`relational/ridgeline_lineage.sqlite`（SQLite，可直接导入分析工具），
同样内容也以 CSV 放在 `relational/csv/`。

| 表/视图 | 作用 |
| --- | --- |
| `image` | 镜像名、SHA-256、字节数、备份序列、恢复父镜像、分叉类型 |
| `run_attribution` | 运行归属：run id、分支名、分析团队、父镜像、修正值 |
| `series_fact` / `sample_fact` | 逻辑观测事实（系列、时间戳、值、提交序列） |
| `block_fact` | 物理块事实（块 ID、起止、点数、字节数、校验和） |
| `rollup_fact` / `rollup_cell` | 报表定义、source_sequence、stale 标志与每个窗口值 |
| `logical_fingerprint` | 逐点规范化后的逻辑指纹，布局变化不影响它 |
| `physical_fingerprint` | 块 ID + 校验和 + 字节数的物理指纹 |
| `rollup_value_fingerprint` | 每个报表全部单元格的值指纹 |
| `rollup_pair_verdict` | 报表对的自动归类（含同序列不同值标记） |
| `image_pair_verdict` | 镜像对的逻辑/物理归类 |
| `same_sequence_different_values` | 标题现场：同 source_sequence、异值的报表对 |

常用查询：

```sql
-- 同序列但不能归并的报表
SELECT * FROM same_sequence_different_values;
-- 布局变化但观测相同的镜像对
SELECT * FROM image_pair_verdict WHERE verdict='SAME_OBSERVATIONS_LAYOUT_ONLY';
-- 追溯任一报表的镜像与分支（完整库）
SELECT r.image_name, r.rollup_name, r.source_sequence,
       a.branch_name, a.analyst_team, a.produced_from
FROM rollup_fact r JOIN run_attribution a USING (image_name);
```

## 4. 拿掉运行归属之后还能证明什么

`relational/ridgeline_lineage_redacted.sqlite` 删除了 branch-a 的
`run_attribution` 行（CSV 版在 `csv_redacted/`）。另附
`ridgeline_lineage_redacted_no_parent.sqlite`，进一步把 `image.restored_from`
置空，模拟归属与结构血缘同时缺失。

**仍可凭现有证据证明（不依赖归属表）：**

- branch-a 与 branch-b 的同名报表 source_sequence 同为 4 但值指纹不同 →
  拒绝归为同一来源（由 sample/rollup 事实与判定视图直接推出）；
- branch-b 的 `pulse-caliber` 查询口径与其他镜像不同 → 口径拒绝；
- base-seed 与 branch-c 逻辑指纹相等、物理指纹不等 → 仅布局变化，
  不是观测变化；
- 每个镜像自身的镜像哈希、块校验和、报表新鲜度（stale=0）与逐点数值；
- branch-a 库内 t=500 的现值是 8.0、提交序列为 4（sample_fact 自带）。

**已不能确认：**

- 这份 branch-a 镜像由哪个团队/哪个分支运行产出（团队、分支名只存在于
  被删的归属行）；
- 它是否「从 base-seed 恢复」而来：仅删归属行时，`restored_from` 仍在，
  结构父链接还可证；在 no_parent 版本中该链接也被摘除，共同备份血缘
  无法从库内证明，外部交接记录成为唯一凭据；
- source_sequence=4 与任何其他镜像的 4 有同源关系——序列号是库内计数器，
  归属缺失时它不携带任何跨库身份信息。

运行 `python3 -I handoff/verify.py <sqlite 路径>` 可得到逐条 PROVABLE /
UNPROVABLE 结论；红色版本中归属类检查显式报告 UNPROVABLE 而非失败。

## 5. 复现方式

```sh
# 需要 Go 1.25+（若 go 不在 PATH，用 GO_BIN 指定）
python3 -I handoff/scenario.py     --scratch handoff/var/run --dist handoff/dist
python3 -I handoff/build_relational.py --evidence handoff/dist/evidence.json --dist handoff/dist
python3 -I handoff/redact.py       --dist handoff/dist
python3 -I handoff/verify.py       handoff/dist/relational/ridgeline_lineage.sqlite
```

- `dist/images/*.db`：四个可直接用 `RIDGE_DB=` 启动的 bbolt 镜像；
- `dist/evidence.json`：全部 API 请求/响应流水与镜像哈希（审计原件）；
- 所有脚本只使用 Python 标准库，读取数据均以 `-I` 隔离模式运行；
- 镜像恢复一致性已在生成时逐库校验（`evidence.restore_checks`）。

### 哈希保全注意事项

bbolt 在进程打开并正常关闭数据库时可能重写文件元数据，即使没有逻辑
写入，文件字节也会变化、SHA-256 随之改变。因此：

- 交付镜像的权威哈希以 `evidence.json` 与 `image.image_sha256` 为准，
  生成流水线结束时已逐文件核对一致；
- 需要启动镜像验证时，请先复制到工作目录再对副本设置 `RIDGE_DB`，
  不要直接打开 `dist/images/` 中的原件；
- 重开后逻辑内容（range、rollup、块清单）仍与关系数据一致，变化的
  仅是物理字节——这本身也是「物理变化不等于观测变化」的又一例证。
