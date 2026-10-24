# 这是2026年秋季学期FBDP课程作业仓库

## 作业5

### 设计思路

程序包含两个依次执行的 MapReduce Job。

#### Job 1：判断集合关系

通过 MultipleInputs 分别读取 A.txt 和 B.txt。

两个 Mapper 将“指数编号 + 股票代码”作为键，
将数据来源 A 或 B 作为值。

Reducer 汇总同一个键的来源，输出：

- BOTH：同时存在于 A 和 B
- A_ONLY：仅存在于 A
- B_ONLY：仅存在于 B

使用来源标记判断归属，因此同一文件中的重复记录不会重复计数。

#### Job 2：按指数汇总

读取 Job 1 的输出，以指数编号为键，
分别统计 BOTH、A_ONLY 和 B_ONLY 的数量。

并集数量 = BOTH + A_ONLY + B_ONLY。

Job 1 成功后才启动 Job 2。

### 输出结果

output_task2/part-r-00000 的字段依次为：

指数编号、交集数量、仅 A 数量、仅 B 数量、并集数量。

| 指数编号 | BOTH | A_ONLY | B_ONLY | UNION |
|---|---:|---:|---:|---:|
| 101 | 3 | 13 | 4 | 20 |
| 102 | 3 | 19 | 3 | 25 |
| 103 | 52 | 195 | 255 | 502 |

### YARN 运行截图

![两个 Job 运行成功](作业05/Project_5/yarn-success.png)