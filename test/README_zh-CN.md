# agilepool 测试工具(插件式)

go-agile-pool 的性能测试工具。完整设计文档与插件编写指南(英文)见
[`docs/test-harness.md`](../../docs/test-harness.md);接入范例见
[`example.go`](example.go)。English version: [`README.md`](README.md)。

## 这是什么

旧实现把所有能力(池参数、任务模型、提交策略、插桩、采样、profiling)平铺在
一个 main 里,靠短 flag 前缀(`-T`/`-U`/`-w`…)区分,歧义多、难组合。重构为
**插件式子命令式**:每个能力是一个独立插件,宿主框架只做"切段、排依赖、按
生命周期调度"。**新增能力 = 新增一个文件 + `plugins.go` 登记一行,框架零改动。**

## 已实现插件

| 段 | 作用 | 关键选项(默认) | 生命周期 |
|---|---|---|---|
| `--pool` | 创建池并持有 | `workers`(20000) `queue`(10000) `container`(linkedlist) `mode`(block) `clean-period`(500ms) | Start 建池 / End `Close` |
| `--task` | 任务时长模型 | `type`(fixed) `base`(10) `extra`(0) `mean`(10) `sigma`(5) | — |
| `--submit` | 提交策略并等排空 | `strategy`(immediate) `num`(1000000) `interval`(10) `jitter`(0) `mean-interval`(50) `phases` `shards`(1) `submitters`(1) | — |
| `--hook` | 事件插桩 | `mode`(none;hook 计数;trace 待上游追踪接线) | — |
| `--metrics` | 周期采样 + 汇总 | `interval`(1s) `format`(csv) `file`(metrics.csv) `wait-exit`(0) | Start 起采样 / End 收尾拍+观察窗+汇总 |
| `--profile` | pprof 包络会话 | `cpu`(false) `mem`(false) | Start 开 CPU / End 停 CPU+写 heap |

### 钩子稳定性压测插件(`h*` 家族)

每个插件都是独立场景:自建私有池、装上对抗性钩子、断言 PASS/FAIL 不变量
(首个 FAIL 即退出码 1)。刻意不声明依赖,`--hcount` 可单独运行:

| 段 | 施压场景 | 关键选项(默认) |
|---|---|---|
| `--hcount` | 精确计数:N 个钩子 × M 个任务,事件不丢不重 | `num`(20000) `hooks`(8) `workers`(1000) |
| `--hpanic` | 钩子 panic 不得击穿提交/执行/Close 路径 | `num`(3000) `level`(dispatch/callback) `stage`(all/…) |
| `--horder` | 单任务事件序(Submitted 首、Started 先于 Completed)+ ctx 载荷 + panic 值透传 | `num`(20000) `panics`(2000) `workers`(1000) |
| `--hctx` | ctx 载荷贯穿全事件;预取消与排队中取消语义 | `num`(3000) `queued`(150) |
| `--hblock` | 慢/阻塞钩子不得死锁或丢事件 | `num`(5000) `delay-us`(200) `workers`(200) |
| `--hchurn` | 分发前多协程并发注册风暴(契约窗口),精确记账 | `num`(10000) `num2`(5000) `churners`(4) `adds`(25) |
| `--hreenter` | 重入分发:钩子内再提交新任务 | `num`(2000) `budget`(2000) `depth`(32) `stage`(submitted/completed) |
| `--hclose` | OnPoolClosed 恰好一次(含并发 Close)+ 关闭后静默 | `num`(2000) `closers`(4) |
| `--henqueue` | 溢出缓冲路径上的 Enqueued 精确记账(queue << num)、慢钩子与重入钩子 | `num`(2000) `reenter`(500) `workers`(1) `queue`(2) `delay-us`(50) `submitters`(1) `rtask-us`(100) `attempts`(8) |

submit 策略:immediate / linear / constant / poisson / phased;任务时长类型:
fixed / uniform / normal。依赖关系:`submit ← pool+task`,`hook ← pool`,
`metrics ← pool`,漏写依赖段时宿主按默认参数自动补全(如只写 `--submit`
会自动带出 pool/task),依赖乱序/重复段/成环都会以退出码 2 报错。

## 快速使用

```text
# 查看插件与帮助
go run . --list
go run . --help submit

# 一个完整基准场景(hook 计数会带进 metrics 采样行)
go run . --pool workers=20000 queue=10000 \
         --task type=fixed base=500 \
         --hook mode=hook \
         --submit strategy=immediate num=200000 \
         --metrics interval=1 format=csv file=out.csv
```

全场景回归脚本(等价旧 run_test,场景逐行对应、输出文件名沿用旧自动命名):

```text
run_test.bat        # Windows
run_test.sh         # Linux/macOS
run_hook_stress.bat # Windows:跑全部 h* 钩子稳定性场景
run_hook_stress.sh  # Linux/macOS:同;run_hook_stress.* race -> -race 构建
```

`test/` 是独立 Go 模块(`github.com/Yiming1997/agilePool/v2/test`),
`replace` 把 `github.com/Yiming1997/agilePool/v2` 指到仓库根目录,工具始终
编译本地库、绝不下发网络版本。

## 约定速览

- 段头 = 命中注册插件名的 `--xxx`,任意位置开新段;段内参数**按原顺序原样透传**
  给插件自解析(`-T fixed --task-base 500` 这类 flag 写法可用),也支持
  `key=value`(框架提供 `ParseOptions`/`GetInt`/`GetDuration`/`GetBool`/`GetFloat`);
- 生命周期:Start(全会话前,按序,带本段参数)→ Run(逐段)→ End(逆序收尾);
  出错即中止但已 Start 的段仍逆序 End;
- 插件间只经共享 Store 交换**会话级对象**(池句柄/配置/工厂);每任务级计数留在
  插件自身原子状态,metrics 采样时按句柄直接读数——共享表不进热路径;
- 退出码:0 成功 / 1 运行错误 / 2 用法错误;错误带 `[插件名]` 前缀;
- 命令行分析、执行模型、数据列与旧格式的对照等细节见
  [`docs/test-harness.md`](../../docs/test-harness.md)。

## 目录

```text
docs/test-harness.md  设计文档 + 插件编写指南(英文,仓库根 docs/)
test/
  README.md        本文档英文版
  README_zh-CN.md  本文档(中文)
  main.go          宿主:切段/规划/生命周期调度/帮助
  plugin.go        接口(Plugin/Optioned/Lifecycle/Depender)+ Runtime
  option.go        key=value 解析与类型化取值
  store.go         会话级共享 Store(Provide/Get/Require)
  registry.go      注册表(重名/依赖注册校验)
  plan.go          依赖规划(自动补全/顺序/环检测)
  cli.go           段切割 + --list/--help
  plugins.go       集中注册表
  example.go       演示插件 provider/consumer(默认不注册)
  pool.go ...      六个正式插件(一文件一插件)
  hookcheck.go     h* 稳定性家族共用机制
  hcount.go ...    九个 h* 钩子稳定性插件(h*.go,一文件一插件)
  run_test.bat     Windows 回归脚本(新语法)
  run_test.sh      Linux/macOS 回归脚本(与 bat 同场景)
  run_hook_stress.bat/.sh  钩子稳定性场景脚本(可带 `race` 参数)
  plot_csv.py      将 metrics_*.csv 画成每个文件一份 SVG(在结果目录运行)
```

代码注释为英文,与库内其他包风格一致;设计文档 `docs/test-harness.md` 为
英文,本 README 提供中英双语。
