# 更新架构决策到 spec 和 PRD

## 背景
经过代码验证和架构讨论，明确了 Prefect 在 dayu-agent 中的真实定位和提取代码的存放位置。

## 要更新到 spec (`backend/index.md`)
1. 新增架构硬约束补充：Prefect 调度层定位——作为四层架构之上的编排层，直接驱动 FinsRuntime/AsyncAgent
2. 说明 Prefect 路径与 CLI/Chat 路径的层级穿透关系

## 要更新到 PRD (`docs/PRD_财报智能处理平台.md`)
1. §1.4：根据代码验证结果，更正"绕过 Host"描述为"不经过 UI→Service→Host 三层，直达 FinsRuntime/AsyncAgent"
2. §5.3 关键决策：同步更新描述
3. 新增提取代码存放位置说明（dayu/fins/extraction/）
