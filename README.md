# Saga Orchestrator — 公开契约（baseline）

**任务编排与补偿**服务：工作流 = 一串步骤，每步可带一个补偿动作；某步失败时，**已完成步骤的补偿按逆序执行**。
本次基线只实现最小可用子集，后续任务在此契约之上继续建设。

## 运行

```bash
PYTHONPATH=src python3 -m saga.app --port 18897 --db saga.sqlite
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

Python 3.12，**仅标准库**；`127.0.0.1`；实例与事件账本持久化在同一个 sqlite 文件（WAL 无关，单文件，
`CREATE TABLE IF NOT EXISTS` 自动迁移，旧文件可直接打开）；`:memory:` 供测试。

## 概念

- **工作流定义**：`{"steps":[{"name":"reserve-stock","compensation":"release-stock"}, ...]}`（1–20 步）。
- **实例状态**：`{"status":"running|completed|compensated", "step": <当前步骤名或 null>, "index": <int>,
  "completed":[<已完成步骤名>], "compensated":[<将/已执行的补偿名，逆序>], "context": {...}, "failure": null|{...}}`。
- 状态转移是**纯函数**：同样的 `(状态, outcome)` 永远得到同样的下一个状态。

## 接口

### `GET /health`
`200 {"status":"ok"}`

### `GET /v1/workflows`
`200 {"workflows": ["order","provision", ...]}`（字典序；内置 `order`、`provision`）

### `PUT /v1/workflows/{name}`
请求体 = 工作流定义 → `200 {"workflow": name, "steps": [...]}`。
定义非法（step 非对象、`name` 空/超 100 字符、未知字段、步数越界）⇒ `400 invalid_request`。

### `POST /v1/workflows/{name}/instances`
请求体：`{"context": {...}}`（可省）→ **`201`** `{"id": <uuid>, "workflow": name, "state": {...}}`。

### `POST /v1/instances/{id}/events`
请求体：`{"outcome": "succeeded"|"failed", "detail": <任意 JSON>, "eventId": <可选>}` →
`200 {"id":..., "workflow":..., "state": {...}}`。
- `succeeded`：推进到下一步；已是最后一步 ⇒ `status="completed"`。
- `failed`：`status="compensated"`，`compensated` = 本步及之前所有**带补偿**的步骤名**逆序**。
- 实例已终态（completed/compensated）再发事件 ⇒ **`409 invalid_transition`**；`outcome` 非法 ⇒ `400`。
- **`eventId`（可选）**：非空字符串、至多 100 字符；在**单个实例范围内**唯一，不同实例不共享命名空间。
  - 不带 `eventId`：保持原有非幂等行为，每次都进入状态机。
  - `eventId` 为 `null`、空串、非字符串或超长 ⇒ **`400 invalid_request`**（显式 `null` 与"省略字段"不同）。
  - 首次合法事件：原子推进状态，并把事件标识、归一化请求内容（`outcome`/`detail`）与该次处理的
    **完整 200 JSON 响应**写入该实例的事件账本（`instance_events` 表，与实例同在一个 SQLite 文件）。
  - 相同 `(实例, eventId)` 再次提交：
    - `outcome` 相同且 `detail` 的 **JSON 值**相同（省略 `detail` 与显式 `null` 等价；键顺序无关）
      ⇒ 原样返回**首次处理的 200 响应**（含当时的 `state` 快照），不改变当前实例状态，
      不重复累计 `completed`、`compensated` 或 `failure`。
    - `outcome` 或 `detail` 不同 ⇒ **`409 invalid_transition`**，状态不变。
  - 非法 outcome、终态推进、实例不存在（`404`）均**不写入**事件账本，因此未占用 `eventId`。
  - 账本随 SQLite 文件持久化：服务关闭后在同一文件上重新打开，重复提交仍命中首次结果。
  - 同一实例的并发事件由进程内串行化保证，结果等价于某个确定的串行顺序；同一 `eventId`
    的并发请求中恰有一个完成首次处理，其余全部得到首次响应。

### `GET /v1/instances/{id}`
`200 {"id","workflow","state"}`；未知 id ⇒ `404 not_found`。

## 错误语义

```json
{"error": {"code": "invalid_request|not_found|invalid_transition|internal_error", "message": "<可读说明>"}}
```

## 未实现（后续任务候选，非固定题单）

步骤重试（无 eventId 时）、超时与死信、跨进程并发与抢占、外部事件等待、编排版本迁移与在飞实例、
分区与顺序保证、账本重放与审计接口、限流与背压、失败注入测试。
