# MIT 6.5840 Raft Labs

这是一个 MIT 6.5840（原 6.824）课程实验代码仓库，包含我在本地完成并通过的 `raft1` 相关实现。

## 仓库分支说明

- `main`：干净单提交版本（便于阅读与展示）
- `raft-old-version`：历史实现版本（用于对比）

## 环境要求

- Go 1.22+（建议）
- macOS / Linux

## 快速开始

```bash
git clone https://github.com/Yunt1/6.5840-raft.git
cd 6.5840-raft/src
go mod tidy
```

## 运行 Raft 测试（3A ~ 3D）

进入 `raft1` 目录后执行：

```bash
cd raft1
go test -run '3A|3B|3C|3D'
```

若要分别运行：

```bash
go test -run 3A
go test -run 3B
go test -run 3C
go test -run 3D
```

## 项目结构（核心）

- `src/raft1/`：Raft 核心实现（3A~3D）
- `src/labrpc/`：课程 RPC 框架
- `src/labgob/`：课程编解码工具
- `src/tester1/`：测试框架与辅助组件
- `src/raftapi/`：Raft 接口定义

## 说明

- 该仓库用于课程学习与实验记录。
- 如果你要复现实验结果，建议直接基于 `main` 分支运行测试。
