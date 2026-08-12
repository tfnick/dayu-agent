# PRD 架构策略回退

## 决策
Prefect 自动提取直接构建 AsyncAgent，绕过 Host。作为有意偏离 CLAUDE.md "Host 是 Agent 生命周期强约束真源" 的架构决策，在 PRD 中显式记录。

## 改动范围
1. 撤销上一轮 PRD 中所有"经 Host operation 通道"的架构修改，恢复原文
2. 新增 §1.4 章节：架构偏离说明，记录决策与理由
3. Prefect 负责 extract task 的 LLM 并发治理（替代 Host llm_api lane）
4. §2.1/§4.1/§4.2/§5.2/§5.3/§5.4.5/§5.5.1/§5.5.5/§5.7/§8.3 全部回退为原始文本
