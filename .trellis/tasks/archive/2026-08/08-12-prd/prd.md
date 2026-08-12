# 修订 PRD 文档（2026-08-12）

## 背景

对 `docs/PRD_财报智能处理平台.md` (v5.5) 做了一轮审查分析，发现内部矛盾、架构冲突、技术风险等问题。用户针对关键问题做出决策，现在需要将决策落地到 PRD 文档中。

## 用户决策（三个关键点）

1. **架构决策**：自动提取改为走 `FinsRuntime` 的 operation 通道，让 Host 保留 run 记录；Prefect 只负责重试/并发/调度。不走绕过 Host 直调 AsyncAgent 的路径。
2. **统筹修改**：由我来完整修订 PRD，修复所有已识别问题。
3. **MinerU 对接方式**：不部署本地 MinerU，改为对接 MinerU 云端精准解析 API（https://mineru.net/apiManage/docs）。

## 需要修订的要点

### 架构变更
- §4.1/§4.2/§5.3/§5.5.5：自动提取从"绕过 Host 直调 AsyncAgent"改为"走 FinsRuntime operation 通道 + Host run record + Prefect 调度"
- §2.1 架构图：ExtractRunner 与 Host 的关系重新画
- §2.2 分层职责表：Extraction 层描述修正

### MinerU 替换方案重构
- §3（全部）：从"本地 magic-pdf 库"改为 MinerU 云端 API 调用
- §3.2 调用链：API HTTP 调用替代本地 PDF 拆分→MinerU 转换→合并
- §3.3 新建文件：`mineru_export.py` 内部改为 HTTP API 客户端封装（不再需要 PDF 拆分/合并逻辑）
- §3.6 风险：移除 MinerU 环境部署风险，新增 API 限流/超时/费用风险评估
- §7.1 工作量：MinerU 模块行数调降（API 封装比本地引擎简单）

### SQL 数据完整性修复
- §4.7.4：`INSERT OR REPLACE` 改为 `INSERT ... ON CONFLICT DO UPDATE`，保留主键不变
- §4.7.7：`upsert_metric` 的 upsert 语义修正，Chat 重新提取不破坏已有 corrections FK

### 内部矛盾修复
- §4.4/§4.5/§4.7.7：统一 source 语义（auto_extracted / chat_extracted；修正走 corrections 表，不设 chat_corrected source）
- §4.3/§4.7.7/§8.3/§8.5：修复章节编号重复

### 工作量与杂项
- §7.2/§7.3：工作量数字对齐（4.5 周）
- §4.7.2：清理残留的"当前 PRD 方案"字样
- 路径简写统一：保持 `prompts/` 简写（与 dayu-agent 内部习惯一致，不改动）

### 技术风险补充
- DuckDB 并发写冲突处理机制（§6.3）
- Prefect task 与 dayu-agent 锁的兼容性说明（§5.5.3）
- 自动提取失败率监控建议（§4.2）
