# 补报批次拒绝差异：对照实验与处置结论

同一批补报点在两种提交顺序下报出不同错误的重现、归因与拆批补送方案。
对照脚本：`examples/backfill_order.py`（临时库、自包含断言，可直接重跑）。
本文结论全部由该脚本在真实 API 上验证，全部为确定性行为，与时钟偏差无关。

## 设置

- 命名空间策略：`retention_ms=2000`、`lateness_ms=1000`、`future_ms=500`。
- 初始：`now=10000` 写入 `a@10000`（watermark 变为 10000），随后用同一
  `now=10000` 执行 retention（floor 变为 `10000-2000=8000`，watermark 不变）。
- 补报批次（`now=10000`，四个点）：`b@8500`、`b@7900`、`a@10600`、`c@9500`。

## 判定规则（internal/policy/admit.go）

每个请求原子生效；点按**请求内顺序**逐点检查，任一失败即整批回滚：

| 顺序 | 检查 | 条件 | 报错 detail |
| --- | --- | --- | --- |
| 1 | retention floor | `t < floor`（此处 8000） | `point precedes retention floor` |
| 2 | future skew | `t - now > 500` | `point exceeds future skew policy` |
| 3 | lateness | `watermark - t > 1000`（此处 watermark=10000） | `point exceeds out-of-order time policy` |

均为 HTTP 409 / `conflict`。lateness 参照的是**此前已接受的最大时间戳**
（watermark），不是墙钟；空命名空间没有 watermark，不做该检查。

## 逐点判定（now=10000、watermark=10000、floor=8000）

| 点 | floor | future | lateness | 单独提交的结论 |
| --- | --- | --- | --- | --- |
| `b@8500` | 通过 | 通过 | 迟到 1500 > 1000 | 409 out-of-order |
| `b@7900` | 7900 < 8000 **违反** | 通过 | 迟到 2100 > 1000 **也违反** | 409 floor（lateness 被掩盖） |
| `a@10600` | 通过 | 超前 600 > 500 | 不适用 | 409 future |
| `c@9500` | 通过 | 通过 | 迟到 500 ≤ 1000 | **接受** |

## 结论一：哪些原因是重叠的

`b@7900` 同时违反 floor 与 lateness 两条**相互独立**的规则；由于检查顺序是
floor 在前，报错只显示 floor，lateness 原因被掩盖。对照（场景 s3）：不执行
retention 的命名空间里，同一个点报的就是 lateness。floor 与 lateness 数值区间
恰好重叠（floor=8000、lateness 下界=9000），容易误判为同一原因。

## 结论二：首个错误掩盖了什么

整批原子拒绝意味着首个错误掩盖了两类事实：

1. 同批其余点的真实原因不再暴露。同一批四个点，仅因顺序不同，首个错误可以是
   out-of-order / future / floor 三种中的任意一种（场景 s1 三种顺序三种报错）。
   **两台代理报错不同的全部原因就在于此，不需要时钟偏差来解释。**
2. 批次里**本可接受的点被一起丢弃**：`c@9500` 单独提交能过（场景 s2），但夹在
   被拒绝的批里时不会落库。

## 结论三：哪些取决于之前已接受的写入

- **lateness 拒绝依赖已接受的写入**：watermark=10000 来自先前接受的
  `a@10000`。对照（场景 s3）：无 watermark 时 `b@8500` 直接被接受。
- **floor 拒绝依赖此前执行过的 retention**：floor=8000 由 `now=10000` 的
  retention 推进，单调只升、不可回退；它与 watermark 无关（无 watermark 时
  `b@7900` 仍被 floor 拒绝）。
- **future 拒绝只依赖调用方传入的 `now`**，与库内状态无关。这是唯一与"时钟"
  相关的项：`now` 到达 10100 后 `a@10600` 的 future 拒绝自然消失。

值班的两个猜测中："旧数据拖累别的设备"**部分属实**——已接受的 `a@10000`
（watermark）和已执行的 retention（floor）正是 `b` 系列被拒的原因；"时钟偏差"
只对 `a@10600` 的 future 项成立。

## 结论四：拒绝是否留下痕迹

不留。每个 409 前后都做了快照对照（场景 s1–s4 共 11 次拒绝）：**不产生点、
不创建新系列、watermark / floor / last_sequence 全部不变**。被拒请求可以安全
重试，无需清理。

## 结论五：时钟到 10100 后的拆批补送（接手者必读）

`now=10100` 时 future 上限变为 10600，但拆批顺序决定 `c@9500` 的生死：

| 补送方式 | 结果 |
| --- | --- |
| 先发 `a@10600`，再发 `c@9500` | `a` 接受且 watermark 升至 10600；`c` 迟到 1100 > 1000，**被拒** |
| 先发 `c@9500`，再发 `a@10600` | 两者都接受（`c` 判定时 watermark 仍是 10000） |
| 两点合并为**一个**请求（任意顺序） | 两者都接受（整请求按请求前 watermark 一次性判定） |

因此「单独试时能过」≠「另一批先写成功后仍能过」：`c@9500` 单独试能过，
但 `a@10600` 先落库后就过不了。**安全做法：先发 `c@9500` 再发 `a@10600`，
或把两点合并在同一个原子请求里。**

`b@8500` / `b@7900` **等时钟永远无效**，不要重试：

- lateness 相对 watermark 而非墙钟：`b@8500` 在 `now=10100` 甚至 `now=20000`
  下仍被拒（场景 s4）；`a@10600` 落库后 watermark 更高，只会更糟。
- floor 单调不可逆：`b@7900` 在任何 `now` 下都被 floor 拒绝。
- 补送时若再执行 retention（`now=10100` → floor 8100），结论不变：
  8500 ≥ 8100 仍卡 lateness，7900 < 8100 仍卡 floor。
- 本 API 没有修改命名空间策略或回退 floor 的入口；这两个点只能丢弃，
  或在新建命名空间（更大 lateness_ms / 未推进的 floor）中重报。

## 重跑

```sh
python3 examples/backfill_order.py --output var/backfill_order.json
```

脚本在临时目录构建并启动服务，跑完全部 4 个场景（14 项断言、约 140 个
API 操作），任一结论不成立即非零退出；`--output` 保存全部请求/响应留档。
