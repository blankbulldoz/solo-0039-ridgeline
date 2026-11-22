# Ridgeline 归约数值实验

只读实验，用于量化 `internal/window` 中 float64 归约（sum / avg / 分组
rate）的舍入行为、溢出处理与可重复性。**不修改、不被任何生产代码导入。**

分析结论见 [`NUMERIC_ANALYSIS.md`](../NUMERIC_ANALYSIS.md)。

## 目录结构

| 路径 | 作用 |
| --- | --- |
| `numerics/lib/` | 纯函数库：顺序/两两/Kahan/Neumaier/排序/big.Float 求和、rate 同构实现、误差与 ULP 指标、确定性场景生成 |
| `numerics/` | 微基准命令：策略对比、结合顺序探针、rate 用例、溢出粘性用例 |
| `numerics/e2e/` | 驱动真实引擎（bbolt + 真实 `window.Aggregate/Rates`），区分舍入漂移 / 业务定义 / 数据可见性 / 物理布局可重复性 / 溢出 |
| `numerics/sweep/` | 误差随点数增长曲线、2^53 吸附阈值 |
| `numerics/roundtrip/` | JSON 与 gzip block 持久化的逐位还原校验 |
| `numerics/out/` | JSON 报告（已含一次参考运行结果） |

## 运行

```sh
# Go 1.25+；如 go 不在 PATH，可用 GO=/path/to/go bash run_experiments.sh
bash experiments/run_experiments.sh
```

所有输入均为确定性生成（固定 LCG 种子，无随机全局状态）。重复运行时，
除报告内的墙钟时间戳与 `nanos_per_op` 外，数值字段逐位一致。

## 三类现象如何被隔离

- **舍入漂移**：同一批引擎实际可见点，输出 vs big.Float(128bit) 参考值
  （e2e 的 F1、F4b；微基准全部场景）。
- **业务定义差异**：同一批数据换定义——avg 观测加权 vs 序列等权（F2）、
  rate 复位规则 vs 朴素差商（F3）、非负约束（F4）。
- **数据可见性差异**：同一查询改可见集合——墓碑删除（F6）、同时间戳
  纠正（F7）、保留期地板（F9）；并用密封（F5）与压缩（F8）证明物理
  布局变化不改变任何单元格。
