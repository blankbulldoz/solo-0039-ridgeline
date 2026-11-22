# 窗口聚合数值正确性保障

本文档是窗口聚合（`internal/window` 及其引擎路径）数值测试套件的依据说明：
测试的独立性论证、数值容差推导、物理布局不变性方法和反例目录。

## 运行

```sh
go test ./internal/window/ ./internal/engine/ -count=1
```

全部测试可重复：无网络访问、无真实时钟依赖、随机置换使用固定种子
（`rand.NewSource(20261122)`）、数据库文件落在 `t.TempDir()`。

## 交付物

| 文件 | 内容 |
| --- | --- |
| `internal/testutil/oracle/oracle.go` | 独立参考实现：`big.Int` 欧几里得除法做桶归属，`big.Rat` 精确有理数做归约 |
| `internal/window/oracle_test.go` | 归属不变量穷举、边界表、全函数 oracle 对比、输入排列不变性、rate 语义 |
| `internal/window/counterexample_test.go` | 反例目录运行器（三方三角验证） |
| `internal/window/testdata/counterexamples.json` | 13 条手工推导的反例数据，每条附带"看似合理"的错误输出 |
| `internal/engine/layout_test.go` | 四种物理布局下原始可见性与聚合结果的位级一致性 |

## 独立性论证

要求：期望值不得来自 `window.Aggregate` 或 `window.Bucket` 的输出。本套件用
三条相互独立的途径产生期望，任何两条一致都不足以通过，必须三方一致：

1. **手工推导的数据**（`counterexamples.json`）。每个期望值以有理数字面量
   （`"6"`、`"3/10"`、`"8/3"`）手写，附推导注释，从未运行被测代码获得。
2. **Go 有理数 oracle**（`internal/testutil/oracle`）。与 `internal/window`
   零代码共享，且关键语义刻意采用不同构造：
   - 桶归属用 `big.Int.Div`（欧几里得除法，对正除数即向下取整），而被测
     实现用截断除法加负余数修正；oracle 自身再被两条定义性不变量穷举验证
     （`start ≤ t < start+step` 且 `(start-origin) ≡ 0 (mod step)`，这两条
     性质在半开区间下唯一确定桶起点，不依赖任何计算公式）；
   - first/last 通过对 `(time, seriesID)` 排序取端点得到，而被测实现用
     运行比较；
   - sum/avg/rate 用 `big.Rat` 精确累加，输入是 float64 的精确二进制值
     （`big.Rat.SetFloat64`），因此期望相对实现实际看到的输入是精确的。
3. **被测实现**。必须同时与 1、2 一致。

此外 `TestBucketBoundaryTable` 和 `TestLayoutHandComputedAnchors` 把若干
期望值直接以手算字面量钉在测试里（如 east `[-993,7)` 的 avg = 2.375、
rate = 13/3），不经过任何代码计算。

## 验证的语义契约

1. **半开归属（负时间 + 非零 origin）**：对每个 `(t, origin, step)`，窗口
   起点满足 `start ≤ t < start+step` 且 `start ≡ origin (mod step)`；穷举
   覆盖 origin ∈ {-14..14}、step ∈ {1..16}、t ∈ [-60, 60]，外加平移一致性
   （`Bucket(t+step) = Bucket(t)+step`）和 15 个手算边界（负侧格点、格点
   前后 1ms、负 origin、MinTime）。
2. **分组平均按观测计权**：`avg = Σv / Σn`，不是各序列均值的均值。反例
   `average-weights-observations-not-series` 中 3 点对 1 点，正确值 4，
   序列加权错误值 6。
3. **first/last 同时间决胜**：同一 `(分组, 窗口)` 内时间戳并列时，first 取
   最小 series ID 的值，last 取最大 series ID 的值。这是从实现中提取并
   固化的确定性契约；`TestAggregatePermutationInvariance` 用 25 组固定种子
   排列证明结果与到达顺序无关（整数值使和精确，输出必须逐位一致）。
4. **reset-aware rate**：增量为 `Σ max(Δ, 重置)`，下降视为归零重置并计入
   新值；分母是窗口内首末观测的 elapsed 秒，不是窗口宽度；单观测窗口不
   产出；分组 rate 是各序列 rate 之和；负值输入被拒绝。
5. **统计不随物理布局变化**：同一逻辑数据集以四种布局构建（全 head；
   多块 seal + 删除 + 重新接纳；再 compact；小块交错 + 跨层修正），
   9 个聚合查询的输出在所有布局间**逐位一致**（见下），且与独立可见性
   模型 + 有理数 oracle 一致。

## 数值容差依据

IEEE-754 binary64 的单位舍入 `u = 2^-53 ≈ 1.11e-16`。

- **顺序求和**：n 项求和的最坏绝对误差为 `(n-1)·u·Σ|xᵢ|`（Higham,
  *Accuracy and Stability of Numerical Algorithms*, 第 4 章）。
- **avg**：求和误差外加一次正确舍入除法，误差 ≤ `u·|result| ≤ u·Σ|xᵢ|`。
- **rate**：增量为非负项求和（同上界）；elapsed 是整数毫秒差，转 float64
  精确；最后一次除法 ≤ `u·|result|`。

因此容差取

```
tol = 4 · (terms+1) · u · Σ|xᵢ|
```

其中 `terms` 是该单元累加的项数上界，`Σ|xᵢ|` 由 oracle 以有理数精确给出；
因子 4 覆盖期望值自身转 float64 比较时的舍入。`Σ|xᵢ| = 0` 时容差为 0
（全零项的和必须精确为零）。

**精确比较域**：`count`、`first`、`last`、`min`、`max`、窗口边界不涉及浮点
运算（选择或整数），要求逐位相等 —— 这些量上的任何偏差都是离散错误。

**离散归属错误与浮点误差的区分**：夹具保证单个样本误归属引起的值变化
≥ 0.1（多数为 1），而容差量级为 `1e-13 · Σ|xᵢ|`（本套件数据下 ≤ 1e-12），
判别余量 ≥ 1e9。反例 `decimal-tenths-sum-needs-rational-tolerance` 展示
另一侧：`0.1+0.1+0.1` 在 binary64 下确定性地等于 `0.30000000000000004`，
与精确的 3/10 差 1 ulp —— 这是浮点舍入而非归属错误；要求它精确等于十进
制 0.3 是**测试写法错误**，该反例以 `compare: exact` 固化这一教训。

**跨布局逐位一致的依据**：引擎 scan 输出按 catalog 顺序（series ID 字典序）
排列序列、按时间排列点，归约按该顺序累加，因此同一逻辑数据集在不同物理
布局下累加顺序相同，float64 结果必然逐位相同。布局间逐位一致是确定性
的强断言，不是近似。

## 反例目录

每条反例给出：看似合理的错误规则 → 错误值，以及正确值。运行器断言实现
等于正确值、且不等于任何错误值（数据必须有判别力）。

| 反例 | 看似合理的错误规则 | 错误值 | 正确值 |
| --- | --- | --- | --- |
| `negative-delta-floor-attribution` | `(t-origin)/step` 截断除法 | t=-4 落入 `[-3,7)`，合并为 12 | `[-13,-3)`=5，`[-3,7)`=7 |
| `upper-edge-belongs-to-next-window` | 上界点归入被它关闭的窗口 | `[0,10)`=3，`[10,20)`=6 | 1、2、4 各占一个窗口 |
| `origin-shifts-the-lattice` | 忽略 origin 按 epoch 对齐 | `[-7,0)`=3，`[0,7)`=12 | `[-10,-3)`=1，`[-3,4)`=6，`[4,11)`=8 |
| `average-weights-observations-not-series` | 各序列均值的均值 | 6 | 4 |
| `first-tie-broken-by-smaller-series-id` | 并列按到达顺序 | 9 | 5 |
| `last-tie-broken-by-larger-series-id` | 并列按到达顺序 | 7 | 3 |
| `rate-reset-counts-as-zero-plus-restart` | last−first / 负增量丢弃 | 8/3、5 | 6 |
| `rate-denominator-is-observed-elapsed` | 窗口宽度做分母 | 3/5 | 6 |
| `rate-single-point-windows-emit-nothing` | 单观测窗口也产出 | 1/2、9/10 | 无单元 |
| `decimal-tenths-sum-needs-rational-tolerance` | 精确十进制金标准（测试 bug） | 要求 == 0.3 | 3/10 ± 容差 |
| `group-key-omits-absent-labels` | 缺失标签落成空串键 | `{"zone":""}` | `{}` |
| `min-seeded-from-first-value-not-zero` | min 累加器初始化为 0 | 0 | -5 |
| `max-seeded-from-first-value-not-zero` | max 累加器初始化为 0 | 0 | -2 |

这些错误值"看似合理"的原因：截断除法给出的起点仍在 origin 格点上；
序列加权均值在每组序列等权时与正确值相同（本套件的 `windowDataset`
正是等权情形，只有反例数据能区分）；到达顺序决胜在单批次写入时恰好
与正确值相同；last−first 在计数器无重置时与正确值相同。

## 变异测试记录

为验证套件能抓住上述错误，对实现临时植入 5 个变异，全部被检出并已还原：

| 变异 | 检出测试 |
| --- | --- |
| 删除负 delta 的向下取整修正 | `TestBucketHalfOpenLattice`、`TestBucketBoundaryTable`、`TestCounterexamples`、oracle 对比等 6 项 |
| first 并列改为到达顺序 | `TestCounterexamples`、`TestAggregatePermutationInvariance` |
| rate 重置增量归零（不计入新值） | `TestCounterexamples`、`TestRateMatchesRationalOracle`、引擎布局测试 |
| rate 分母改为窗口宽度 | `TestCounterexamples`、`TestRateMatchesRationalOracle`、引擎布局测试 |
| avg 改为序列加权 | `TestCounterexamples`、引擎布局测试（不等权分组查询） |

## 已知边界

- `window.Rates` 要求每个序列的点按时间排序输入（引擎 scan 的契约）；
  单元测试遵守同一契约，乱序输入不属于该函数的约定范围。
- 容差是关于 `Σ|xᵢ|` 的绝对界。对消主导的数据（数量级相抵的异号大数）
  会天然放宽绝对容差，这正是该误差模型的性质；夹具使用温和量级，
  保证判别余量 ≥ 1e9。
- 引擎级测试覆盖 head/块/compact/修正/删除的布局组合；保留策略
  （retention floor）对可见性的截断不在本套件范围。
