# 样本污染追踪 API（Contamination Trace API）

实验室生物安全员依据样本转运记录划定污染追踪范围。本服务接收样本与转运批次
（起点、终点、离库/入库 UTC 时间、批次号）以及污染源样本，按时间顺序计算污染
可达样本；返回每个受影响样本的**最早到达时间**、**完整路径**和**隔离截止时间**。

更正批次时必须携带客户端所见版本（`seen_version`），服务以新版本替换旧记录，
并使依赖旧版本的已发布结果失效。查询在受理时固定快照，失败时不会发布半份结果。

---

## 1. 启动

### Docker Compose（推荐）

```bash
docker compose up --build -d
open http://localhost:8000/docs      # 交互式 API 文档（Swagger UI）
curl http://localhost:8000/healthz
```

数据保存在命名卷 `trace-data`（容器内 `/data/trace.db`），重启后保留。
停止：`docker compose down`；同时删除数据：`docker compose down -v`。

### 本地运行

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt
TRACE_DB_PATH=./data/trace.db .venv/bin/uvicorn app.main:app --port 8000
```

`TRACE_DB_PATH` 默认为 `./data/trace.db`；系统自动创建目录与 SQLite 表。

### 本地示例

```bash
docker compose up --build -d
./examples/run.sh                 # 使用 http://localhost:8000
API=http://127.0.0.1:9000 ./examples/run.sh
```

示例数据在 `examples/intake.json`。脚本每次运行使用唯一的 ID 前缀，因此可在同一
数据库上反复执行。

---

## 2. 追踪语义（必须明确的部分）

一次转运批次是一条**边**：`origin → destination`，在
`[departed_at, arrived_at]` 之间发生（离库/入库均为 UTC）。一个批次可以装载多个
样本（共享批次）。

**污染批次**的定义（二者之一）：

1. 该批次装载了被标记为污染源的样本；
2. 该批次从某站点**离库**的时间 `t_depart` 落在某污染批次在该站**入库**时间
   `t_arrive` 的窗口内：

   ```
   t_arrive <= t_depart <= t_arrive + 24h
   ```

   即“后续转运须在上一站入库后 24 小时内离库”：恰好 24 小时算作可达，早于污染
   入库则不可达。

这是一条**站点级**的传播规则（上一条链的终点必须等于下一条链的起点）。

**受影响样本**：装载于任一污染批次上、且自身不是污染源的样本。

对每个受影响样本返回：

| 字段 | 含义 |
| --- | --- |
| `first_arrival_at` | 与污染材料中该样本的**最早**入库时间 |
| `quarantine_until` | `first_arrival_at + 24h`（隔离截止时间） |
| `path` | 从污染源批次到该样本的**完整批次链**（每条腿含批次号与版本） |

结果按 `first_arrival_at`、再按 `sample_id` 排序。路径的确定性规则：优先最短路径，
其次按 `(arrived_at, batch_no)` 的处理顺序（结果可复现）。

实现为按 `arrived_at` 单遍扫描的闭包（污染入库必然早于后续离库），每个站点用区间
最小值结构查询 24 小时窗口，因此大规模、含热点枢纽的数据集也能在可接受时间内完成。

---

## 3. API 摘要

完整契约与示例见 `http://localhost:8000/docs`（OpenAPI 3）。

| 方法与路径 | 用途 |
| --- | --- |
| `POST /intakes` | 注册样本、转运批次与污染源；单事务原子写入 |
| `GET /dataset` | 查看当前数据集（含每个批次当前版本） |
| `POST /batches/{batch_no}/corrections` | 以新版本替换批次，并使旧结果失效 |
| `POST /traces` | 在固定快照上计算污染可达样本 |
| `GET /traces` | 列出所有已受理查询 |
| `GET /traces/{trace_id}` | 重读已发布的快照结果 |
| `GET /traces/{trace_id}/invalidations` | 查询失效记录 |
| `GET /healthz` | 就绪探针 |

### 3.1 `POST /intakes`

```jsonc
{
  "source_sample_ids": ["S-001"],
  "samples": [
    {"sample_id": "S-001", "batch_nos": ["B-1001"]},
    {"sample_id": "S-002", "batch_nos": ["B-1001", "B-1002"]},
    {"sample_id": "S-003", "batch_nos": ["B-1003"]}
  ],
  "batches": [
    {"batch_no": "B-1001", "origin": "Collection-Lab", "destination": "Hub-North",
     "departed_at": "2026-01-01T08:00:00Z", "arrived_at": "2026-01-01T10:00:00Z"},
    {"batch_no": "B-1002", "origin": "Hub-North", "destination": "Reference-Lab",
     "departed_at": "2026-01-01T20:00:00Z", "arrived_at": "2026-01-01T22:00:00Z"},
    {"batch_no": "B-1003", "origin": "Reference-Lab", "destination": "Archive",
     "departed_at": "2026-01-02T10:00:00Z", "arrived_at": "2026-01-02T12:00:00Z"}
  ]
}
```

- `batches` 也可内联在样本的 `batches` 字段中；两种写法可混用。
- 污染源可用 `source_sample_ids` 或样本上的 `"contaminated": true` 标记。
- 批次号全局唯一，标识一次实体转运；多个样本通过 `batch_nos` 共享同一批次。
- 时间必须带时区（`Z` 或偏移）；朴素时间按 UTC 解释；响应统一输出 `Z`。
- 提交是**幂等且增量**的：完全相同的重复提交不会产生重复记录；同批次号但取值
  不同则返回 `409`，必须走更正接口。

### 3.2 `POST /traces`

```jsonc
{}                                                  // 使用入库时标记的全部污染源
{"source_sample_ids": ["S-001"]}                    // 或本次查询显式指定污染源
```

受理时在**单个写事务**内读取数据集并固定快照（`intake_id` + 每个批次的版本），
之后的计算只使用该快照。返回：

```jsonc
{
  "trace_id": 1,
  "status": "complete",
  "has_sources": true,
  "source_sample_ids": ["S-001"],
  "affected_samples": [
    {
      "sample_id": "S-003",
      "first_arrival_at": "2026-01-02T12:00:00Z",
      "quarantine_until": "2026-01-03T12:00:00Z",
      "path": [
        {"batch_no": "B-1001", "origin": "Collection-Lab", "destination": "Hub-North",
         "departed_at": "...", "arrived_at": "...", "version": 1},
        {"batch_no": "B-1002", "...": "...", "version": 1},
        {"batch_no": "B-1003", "...": "...", "version": 1}
      ]
    }
  ],
  "snapshot": {"intake_id": 1, "accepted_at": "...", "batch_versions": {...}, "batches": [...]}
}
```

**失败不发布半份结果**：所有结果行与 `status=complete` 在同一事务中提交；任何异常
都会回滚事务、删除可能的部分行，并把该查询标记为 `failed`。读取失败查询返回 `500`
且不含任何结果行。进程在发布前崩溃时，遗留超过 10 分钟的 `pending` 查询会在下次
启动时标记为 `failed`（宽限期避免多进程部署时误杀其他 worker 正在计算的任务）。

### 3.3 `POST /batches/{batch_no}/corrections`

```jsonc
{
  "seen_version": 1,
  "origin": "Hub-North",
  "destination": "Cold-Room",
  "departed_at": "2026-01-01T20:00:00Z",
  "arrived_at": "2026-01-01T21:00:00Z"
}
```

- `seen_version` 必须等于当前版本，否则 `409`（乐观锁，防止覆盖他人更正）。
- 更正会插入 `version + 1` 的新记录并将旧记录标记为已取代；新值原样返回并附带
  `invalidated_trace_ids`。
- 固定了旧版本的已发布查询会被记录失效，随后 `GET /traces/{id}` 返回 `409`
  `snapshot_invalidated`（附失效明细），绝不返回过期数据。
- 没有实际变化的更正返回 `409`。

### 3.4 错误格式

所有错误使用统一结构，不使用 FastAPI 默认的 `detail` 字符串：

```json
{"code": "validation_error", "message": "request validation failed",
 "details": {"errors": [{"loc": ["body", "batches", 0], "msg": "..."}]}}
```

状态码约定：`404` 未知样本/批次/查询或无数据集；`409` 版本冲突、快照失效、批次取值
冲突、查询尚未发布（`trace_pending`，不会以空结果冒充“无影响”）；`422` 载荷校验失败
或引用不存在的批次/样本；`500` 追踪失败（含 `trace_id`，且无结果行）。

`POST /traces` 的 `500`/`409` 响应体是统一错误结构（`code` 为 `trace_failed` 或
`snapshot_changed`），`details.trace_id` 可用于 `GET /traces` 中定位该次失败记录。

---

## 4. 数据一致性模型

- SQLite WAL 模式；所有写操作使用 `BEGIN IMMEDIATE` 串行化。
- 批次只追加：更正产生新版本，旧版本保留用于审计。
- 查询快照固定 `intake_id` 与每个批次的版本；发布前会再次校验这些版本未变化，
  若已变化则拒绝发布（`409 snapshot_changed`，查询标记为 `failed`）。
- 快照读取、结果读取、失效标记读取都在单个读事务内完成，不会返回“半新半旧”的组合。

## 5. 自测

```bash
.venv/bin/python -m pytest -q
```

覆盖范围（48 个用例）：

- 24 小时窗口边界：恰好 24 小时可达、超出 1 微秒不可达、污染入库前离库不可达；
- 多跳链、共享批次、多个污染源、污染源重写；
- 最早到达时间与完整路径（含确定性并列规则）、隔离截止时间；
- 更正替换、连续多版本、陈旧 `seen_version`、无变化更正、未知批次；
- 更正使旧查询失效、旧查询返回 `409`、新查询反映新值、未被引用的批次更正；
- 发布失败/计算异常时不产生任何结果行，且不影响其他查询；
- 受理与更正并发时不会返回半份结果；
- 400 跳深链、5 万批次宽图、2 万批次热点枢纽的性能回归；
- 400 组随机图与独立暴力参考实现逐一比对（同一 `first_arrival` 与路径）；
- 真实 uvicorn HTTP 端到端（`/docs`、入库、追踪、更正、失效、错误结构）；
- 重启持久化、崩溃遗留 `pending` 查询的恢复。

Docker 构建的验证情况见下方“已知限制/未验证项”。

## 6. 已知限制与未验证项

- 本环境**未安装 Docker**，无法执行 `docker build` / `docker compose up`。Dockerfile
  与 compose 文件按标准用法编写，并已用相同的 `uvicorn app.main:app` 命令在本机
  验证；容器内 `pip install` 与健康检查命令未实测。
- 隔离截止时间按“首次到达 + 24 小时”计算，未接入具体病原体的潜伏期参数。
- 未实现认证/授权、速率限制与多租户隔离；默认部署应置于受信网络之后。
- 更正只针对活跃数据集的批次；数据集的整批替换接口未开放（提交是增量式的）。
- 路径选择：优先最短链，其次 `(arrived_at, batch_no)` 处理顺序；这是可复现的确定性
  规则，但不是唯一的合理规则（例如按“最早离库”选路径会得到不同但同样合法的链）。
  若业务要求另一种并列规则，需要在 `app/tracing.py` 中调整。
- 同一批次号在两次入库提交中被定义成不同取值会返回 `409`；必须先更正，不能静默覆盖。
- SQLite 适合单机部署；多副本/高并发写入需要换成服务端数据库。
