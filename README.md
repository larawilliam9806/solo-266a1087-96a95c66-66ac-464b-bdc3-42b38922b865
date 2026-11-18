# 传感器入库排查 · 只读 SQL 工作台

数据工程师排查传感器入库任务重复采样问题的内部工具：在浏览器中上传 SQLite
落地库副本，查看表结构，执行 **只读** `SELECT` / `WITH` 查询，依据结果修正入库逻辑；
并可把现场库 **代号化脱敏** 后导出给外部协作方排查，设备编号与安装位置不外流。

## 快速开始（Docker Compose）

```bash
docker compose up --build
# 打开 http://localhost:8000
```

页面上可直接点击 **「使用内置演示库」** 加载传感器演示数据（含故意制造的重复采样），
或 **「下载演示库」** 后走一遍真实上传流程。

## 本地开发（不用 Docker）

```bash
pip install -r requirements.txt
python3 demo/make_demo_db.py            # 生成 demo/demo_sensors.db
cd webapp && python3 app.py             # http://localhost:8000
```

生产模式（与镜像内一致）：

```bash
gunicorn --pythonpath webapp --bind 0.0.0.0:8000 --workers 1 --threads 16 app:app
```

> 查询名额是进程内信号量，**必须单 worker**（多线程没问题）。

## 代号化导出（外发脱敏）

页面第 ③ 区「生成并下载代号化副本」（`POST /export`）用于把现场库加工成可交给外部
协作方的脱敏库，规则与保证如下：

| 需求 | 实现 |
| --- | --- |
| 仅受理 ≤ 10MB、与演示库同结构的两表库 | 入口先按文件大小拒绝；结构白名单逐列比对 `sensors` / `sensor_samples`（列名/类型/非空/主键/默认值/外键/AUTOINCREMENT），额外用户表、视图、触发器、虚拟/影子表、列或主外键差异、坏库、悬空外键一律拒绝 |
| 每个 sensor_id 生成唯一随机代号并同步改写两表 | `secrets.token_hex` 生成 `SN-XXXXXXXXXX` 代号，去重且不与任何原编号碰撞；两表通过同一映射改写 |
| 位置替换为「已隐藏」 | `sensors.location` 统一写为 `已隐藏`，并校验成品中不存在其他值 |
| 保留其他采样字段、主外键关系和重复次数 | 全新库按演示库结构重建；`id/ts/value/ingested_at/batch_id/metric/unit` 与索引原样搬运，外键开启校验，重复组数与多余行数逐一对账 |
| 保留字段含原编号/位置则拒绝 | 对所有非改写列（含动态类型下可能存文本的 `value`）逐值子串扫描原编号与原位置，命中即拒绝，成品下载前再做一次整文件字节级复核 |
| 活动数据和空闲页都不得留旧值 | 不原地改写：先对当前库做一致性快照（backup API + `VACUUM INTO` 单文件，合并并消除 WAL/空闲页），再写入**全新创建**的成品文件；最后对成品做原始字节扫描，确认无任何旧编号/旧位置、无 WAL/SHM/journal 旁路 |
| 原副本仍可查询 | 源库全程以 `mode=ro` 只读打开，处理路径不持有任何写句柄；自测含导出后查询与文件哈希校验 |
| 受理快照不受换库/清库影响 | 受理瞬间即快照到独立工作目录，之后会话上传替换或清除当前库都不会混入另一版 |
| 失败不提供半成品下载 | 任何一步失败只返回 JSON 错误并删除工作目录；仅全流程（快照→校验→改写→逻辑复核→字节复核）成功才返回文件流，响应结束后工作目录一并删除 |

关键限制常量集中在 `webapp/anonymizer.py`：`MAX_EXPORT_BYTES`（10MB）、
`HIDDEN_LOCATION`（已隐藏）、`CODE_PREFIX` / 代号随机长度。

## 功能与约束的对应关系

| 需求 | 实现 |
| --- | --- |
| 上传 SQLite 副本 | `POST /upload`，校验 SQLite 文件头，按会话隔离存储，可随时清除 |
| 查看表结构 | `GET /schema`：表/视图/索引、列定义、行数 |
| 仅 SELECT / WITH | 文本预检（跳过注释取首关键字）+ SQLite authorizer 双层约束 |
| 不得修改原库 | 连接以 `file:...?mode=ro` 只读打开；authorizer 拒绝 INSERT/UPDATE/DELETE/DDL/ATTACH/PRAGMA 写；自测含写尝试前后行数与文件哈希校验 |
| 不得访问其他文件 | authorizer 拒绝 `ATTACH`；无法通过 SQL 触达库外文件 |
| 不得加载扩展 | `enable_load_extension(False)` + authorizer 拦截 `load_extension()` 函数 |
| 最多 2 条并发，满额明确拒绝 | 信号量名额（默认 2，不排队），满额返回 **429** 与中文提示 |
| 用户可取消 | `POST /query/<id>/cancel`：`conn.interrupt()` 真正中断计算，join 线程后才响应 |
| 超 3 秒自动终止 | 每条查询启动定时器，超时走同一 interrupt 路径 |
| 终止前停止计算并释放名额 | worker 线程**先关闭连接、释放名额，再对外落定终态**；看到 `timeout`/`cancelled` 时计算必然已停、名额必然已还 |
| 其他查询和原库不受影响 | 每条查询独立连接，`interrupt()` 只作用于本连接；库文件只读 |
| 失败不留半份结果 | 结果完整取回后才挂载；任何失败只返回错误信息 |
| 最多 1000 行并提示截断 | `fetchmany(1001)` 判断截断，页面显示截断横幅 |

关键参数均可通过环境变量调整：`QUERY_TIMEOUT`（默认 3 秒）、`MAX_CONCURRENT`（默认 2）、
`MAX_ROWS`（默认 1000）、`MAX_UPLOAD_MB`（默认 300）、`UPLOAD_TTL_HOURS`（默认 24）、
`SECRET_KEY`（生产必须覆盖）。

## 演示库与排查 SQL

- `demo/make_demo_db.py`：生成 `demo_sensors.db`（6 个传感器 × 6 小时 × 10 秒间隔，
  13260 行；重试批次 `B-20261110-RETRY` 造成 270 组重复采样，共 300 行多余）。
- `demo/find_duplicates.sql`：重复采样排查 SQL 五连——汇总、明细、按批次定位、
  待清理行预览（`ROW_NUMBER()` 去重）、修复后验证。页面内置了其中三条作为一键示例。
- 页面「慢查询」示例（递归 CTE）用于演示 3 秒自动终止与手动取消。

## 目录结构

```
webapp/
  app.py               # Flask 路由：上传/表结构/代号化导出/查询/取消/演示库
  anonymizer.py        # 代号化导出器：受理快照、结构白名单、泄密扫描、改写与字节级复核
  query_manager.py     # 查询执行器：名额、超时、取消、只读 authorizer、行数上限
  templates/index.html # 单页面前端（无外部依赖）
demo/
  make_demo_db.py      # 演示库生成脚本
  find_duplicates.sql  # 重复采样排查 SQL
tests/
  selftest.py          # HTTP 端到端自测（含代号化导出全场景）
  test_anonymizer.py   # 导出核心模块自测（61 项检查，纯标准库）
Dockerfile  docker-compose.yml  requirements.txt
```

## 自测

```bash
# 1. 启动服务（任选一种方式），假设监听 8123
UPLOAD_DIR=/tmp/uploads DEMO_DB=/tmp/demo_sensors.db PORT=8123 python3 webapp/app.py
# 2. 另开终端运行
python3 tests/selftest.py http://127.0.0.1:8123 /tmp/demo_sensors.db /tmp/uploads
# 3. 导出核心模块的纯函数级测试（无需起服务）
python3 -I tests/test_anonymizer.py
```

HTTP 自测覆盖：上传校验、表结构、只读约束（含 `WITH` 伪装写、`ATTACH`、`load_extension`、
多语句）、重复采样 SQL、1000 行截断、失败无半份结果、3 秒自动终止、并发满额 429、
取消真实停止计算并释放名额、其他查询不受影响、库文件哈希不变；以及代号化导出的
成功下载、代号唯一与两表同步、位置全隐藏、重复次数保留、成品字节零旧值、原副本可查询、
无半成品残留，和额外表/视图/触发器/保留字段泄密/超 10MB 等拒绝场景。

模块自测额外覆盖：空闲页旧值清除（含先插后删制造泄露页）、WAL 模式快照、列/外键/
AUTOINCREMENT/影子表结构拒绝、动态类型文本泄密、代号防碰撞、受理后替换/清除当前库的
快照隔离、失败不留成品。

## 已知限制

- 会话与上传副本通过 cookie 关联，重启服务（默认 SECRET_KEY 变化）后需重新上传。
- 上传副本默认 24 小时后由后台线程清理（`UPLOAD_TTL_HOURS`），期间占用卷空间。
- 查询名额为进程内实现，不能多 worker / 多副本水平扩展（如需扩展应改为外部锁，如 Redis）。
- 结果集在内存中物化（上限 1001 行），不会流式输出；超大单行（如大 BLOB）以 hex 显示。
- 代号化导出的 10MB 上限与“演示库同结构”白名单是硬约束（`anonymizer.py` 常量/基准结构），
  结构不同的库无法导出，需先按演示库结构规整。
