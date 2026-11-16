# 实时指标计算与监控大盘平台

面向实时指标计算引擎、多维度下钻分析、告警阈值管理、指标异常检测、数据服务开放与可视化看板配置的数据分析与应用工程平台。

这是一个前后端分离的管理平台：前端 React + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 React + Vite + TypeScript 前端
│   ├── src/views/            每个业务模块一个页面
│   ├── src/api/              统一请求封装
│   ├── src/stores/           会话与筛选状态
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/          每个业务模块一组接口
│   ├── app/services/         业务规则与状态流转
│   ├── app/services/realtimecompute.py  窗口计算、事件幂等、版本与下游同步
│   ├── app/seed_realtime.py  实时指标 / 数据源流 / 大盘 / 阈值可计算样例
│   ├── app/store.py          内存数据仓库（带写事务）与示例数据
│   └── tests/                窗口计算链路行为测试
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

运行测试（会额外安装 pytest 与 httpx）：

```bash
cd backend
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/python -m pytest tests -q
```

### 前端

```bash
cd frontend
npm install
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 实时指标窗口计算链路

`realtimemetric` 模块除了指标定义维护，还负责「事件 → 窗口 → 结果版本 → 下游」的实时计算链路：

```text
迟到/实时事件 ──幂等摄取──► 事件表（数据源流 + 事件编号唯一）
                                 │
                          事件时间窗口对齐
                                 │
                 ┌──────── 重放截止时间（窗口结束 + 延迟容忍）────────┐
                 │ 截止前：事件触发窗口整体重算，生成新结果版本          │
                 │ 截止后：迟到事件只留摄取流水，不再计入窗口            │
                 └────────────────────────────────────────────────────┘
                                 │
                  metric_window_result（当前值）+ metric_window_version（版本流水）
                                 │
          ┌──────────────────────┼───────────────────────┐
      dashboard_window_snapshot               threshold_evaluation
      （大盘只展示可消费窗口）                 （阈值只评估可消费窗口）
```

窗口状态口径：

| 状态 | 判定 | 下游 |
| --- | --- | --- |
| 已到齐 | 覆盖率 ≥ 指标下限，且已封窗或事件数达到预期 | 允许消费 |
| 可容忍迟到 | 覆盖率达标，但仍在重放窗口内且事件未到预期 | 暂不消费，等待补齐 |
| 覆盖率不足 | 去重后事件数 / 预期事件数 < 指标覆盖率下限 | 不消费，大盘保留上一份完整结果并标注滞后 |

数据新鲜度：未封窗窗口按「距窗口内最新数据到达的秒数」判定，超过指标新鲜度阈值即为滞留；
封窗窗口是最终版本，标记为「已封窗」而不是滞留。指标级还会给出「链路滞后窗口数」，
用于发现整条计算链路停止推进（避免大盘长期展示旧窗口却不自知）。

### 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/realtimemetric` | 指标定义列表：按数据源流、窗口、聚合方式、维度字段组组合定位，支持搜索、排序、翻页 |
| GET | `/api/realtimemetric/windows` | 窗口新鲜度视图：三态过滤、新鲜度区间、覆盖率区间、排序与翻页 |
| GET | `/api/realtimemetric/windows/{id}` | 单窗口全链路溯源：版本流水、大盘联动、阈值联动 |
| GET | `/api/realtimemetric/dataset` | 实时指标计算结果数据集（默认只含可消费窗口，带数据集版本与溯源说明） |
| POST | `/api/realtimemetric/compute` | 校验并计算窗口结果；支持 `simulate_failure` 演练事务提交失败 |
| POST | `/api/realtimemetric/events` | 提交事件：重复编号幂等，迟到事件在重放窗口内触发重算 |
| GET | `/api/realtimemetric/batches` | 计算批次流水（成功/失败都可追溯） |
| GET | `/api/dashboard/refresh` | 跨模块联动刷新大盘指标卡，并同步阈值评估 |
| GET | `/api/threshold/evaluations` | 阈值评估记录，每条指向窗口结果版本 |

事务一致性：窗口结果、版本流水、大盘快照与阈值评估在同一个 `store.transaction()` 中提交；
提交失败时整体丢弃，并单独落一条「失败」批次（不带任何窗口结果），接口只会返回回滚提示。

### 前端页面

- 实时指标页分「窗口新鲜度视图 / 指标定义 / 计算批次与数据集」三个页签；
  窗口视图支持按数据源流、窗口范围、聚合方式、维度字段组组合定位，区分三种窗口状态，
  可按新鲜度区间过滤、排序、翻页，并展开单个窗口的版本流水与下游联动。
- 监控大盘页展示由窗口结果数据集驱动生成的指标卡，标注滞后窗口数与链路新鲜度。
- 阈值管理页展示窗口级评估记录，可手工按现行取数链路刷新。


| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 实时指标 | `realtimemetric` | 指标定义、窗口结果 | 指标编号、数据源流、计算窗口秒、聚合函数、维度字段组、覆盖率、数据新鲜度 |
| 监控大盘 | `dashboard` | 大盘配置 | 大盘编号、大盘名称、包含指标组 |
| 多维下钻 | `drilldown` | 下钻路径 | 路径编号、起始维度、下钻层级序列 |
| 阈值管理 | `threshold` | 阈值规则 | 规则编号、关联指标、阈值下限 |
| 异常检测 | `anomalydetect` | 检测模型 | 模型编号、监控指标、检测算法 |
| 数据服务 | `dataservice` | 服务接口 | 服务编号、服务名称、接口路径 |
| 告警历史 | `alarmhistory` | 告警记录 | 记录编号、告警规则、触发时间 |
| 同比环比 | `comparison` | 对比配置 | 配置编号、目标指标、对比类型 |
| 聚合配置 | `aggregate` | 聚合规则 | 规则编号、源数据粒度、目标粒度 |
| 筛选预设 | `filterpreset` | 预设模板 | 预设编号、预设名称、维度条件组 |
| 用户洞察 | `userinsight` | 洞察报告 | 报告编号、分析主题、目标人群 |
| 事件分析 | `eventanalysis` | 分析模型 | 模型编号、事件名称、分组维度 |
| 分群画像 | `segmentprofile` | 画像定义 | 画像编号、分群名称、筛选规则 |
| 趋势预测 | `trendforecast` | 预测方案 | 方案编号、预测指标、算法模型 |
| 可视化模板 | `viztemplate` | 图表模板 | 模板编号、图表类型、数据绑定 |
| 订阅推送 | `subscription` | 订阅配置 | 订阅编号、订阅内容、接收人 |
| 维度映射 | `dimensionmap` | 映射规则 | 规则编号、原始维度名、标准维度名 |
| 数据质量分 | `qualityscore` | 评分规则 | 规则编号、数据集名称、完整性权重 |

## 约定

- 每个模块的前端页面在 `frontend/src/views/<模块>/index.vue`，后端接口在
  `backend/app/routers/<模块>.py`，业务规则在 `backend/app/services/<模块>.py`。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

领域：数据工程
子方向：数据分析与数据应用
