# partition/ — 存储布局评估基准

只读式测量工具：调用现有 `engine` 公开 API 构造两类负载，并用真实块清单做时间分区的
反事实工作量推算。**不修改任何存储代码，也不创建新的磁盘布局。**

## 运行

```sh
go run ./bench/partition --mode full   # 两个固定负载，写 results.json
go run ./bench/partition --mode sweep  # 基数/历史长度扩展曲线，写 sweep.json
```

输出目录由 `--out` 指定（默认 `/tmp/ridgeline-bench`），每类负载的 head、sealed、
delete、retention、ladder、broad-delete 各自使用独立 db 副本，互不污染。

## 负载

- `long_few`：20 个超长系列，200 个密封周期，每周期每系列 1 点 → 4,000 个单点块。
- `short_many`：10,000 个短系列，每系列 1 点，末尾一次密封 → 10,000 个单点块。

## 指标口径

- 耗时：7 次（sweep 为 3–5 次）运行取最小值，前两次作预热。
- `bbolt_page_stats`：读事务走 mmap，没有页分配计数，因此用 `BucketStats`
  的 KeyN/Depth/LeafPages 与精确记录遍历次数表达工作量。
- `time_partition_counterfactual`：按一个密封周期为分区宽度，从真实块清单解析计算
  窄查询块检查数与 floor 下可整段摘除的分区/块数，不实例化任何新布局。
- `retention_ladder`：在同一密封库副本上连续推进 4 档 floor，观测零淘汰时的重写成本。

设计结论见 `docs/partition-decision.md`。
