# Saga Orchestrator — 公开契约（baseline）

**任务编排与补偿**服务：工作流 = 一串步骤，每步可带一个补偿动作；某步失败时，**已完成步骤的补偿按逆序执行**。
本次基线只实现最小可用子集，后续任务在此契约之上继续建设。

## 运行

```bash
PYTHONPATH=src python3 -m saga.app --port 18897 --db saga.sqlite
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

Python 3.12，**仅标准库**；`127.0.0.1`；实例持久化在 sqlite（WAL 无关，单文件）；`:memory:` 供测试。

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
请求体：`{"outcome": "succeeded"|"failed", "detail": <任意 JSON>}` →
`200 {"id":..., "workflow":..., "state": {...}}`。
- `succeeded`：推进到下一步；已是最后一步 ⇒ `status="completed"`。
- `failed`：`status="compensated"`，`compensated` = 本步及之前所有**带补偿**的步骤名**逆序**。
- 实例已终态（completed/compensated）再发事件 ⇒ **`409 invalid_transition`**；`outcome` 非法 ⇒ `400`。

### `GET /v1/instances/{id}`
`200 {"id","workflow","state"}`；未知 id ⇒ `404 not_found`。

## 错误语义

```json
{"error": {"code": "invalid_request|not_found|invalid_transition|internal_error", "message": "<可读说明>"}}
```

## 未实现（后续任务候选，非固定题单）

步骤重试与幂等键、超时与死信、并发与抢占、外部事件等待、编排版本迁移与在飞实例、分区与顺序保证、
持久化恢复与重放、限流与背压、可视化查询与审计回放、失败注入测试。
