# A股复盘机器人（RSS -> LLM -> 飞书）

该项目每天定时生成一份“行情复盘与重点信息梳理”，结构对齐你历史 docx：

1. 标题：X月X日行情复盘与重点信息梳理
2. 一. 行情复盘（2段）
3. 二. 市场要点（固定8条，每条“总结标题 + 原文段落 + LLM总结要点”）
4. 三. 建议（基于市场要点做泛化总结）

补充规则：
- 只有清洗后的原文段落长度 `>=100` 字，才会进入市场要点候选池
- 市场要点不展示日期、时间、媒体来源、链接等元数据

## 1）安装

```powershell
cd D:\桌面\ashare_morning_bot
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## 2）配置

```powershell
copy .env.example .env
```

编辑 `.env`：
- `FEISHU_WEBHOOK_URL` 必填
- `OPENAI_API_KEY` 建议填写（用于解读与建议）
- `OPENAI_BASE_URL` / `OPENAI_MODEL` 按你的供应商设置

## 3）运行

```powershell
python main.py
```

成功后会输出：
- 本地文件：`report_YYYY-MM-DD.md`
- 并尝试推送到飞书

## 4）飞书字段说明

脚本会发送：
- `content.text`：完整可复制正文（推荐直接使用）
- `doc_title` / `market_review` / `basis_review` / `key_points` / `strategy`

## 5）Windows 定时任务

- 程序：`D:\桌面\ashare_morning_bot\.venv\Scripts\python.exe`
- 参数：`main.py`
- 起始于：`D:\桌面\ashare_morning_bot`
- 触发：每天（例如 08:30）

## 6）GitHub Actions

项目已提供 workflow：
- [.github/workflows/daily-report.yml](/d:/桌面/ashare_morning_bot/.github/workflows/daily-report.yml)

用途：
- 每天自动运行一次 `python main.py`
- 并支持在 GitHub 页面手动点 `Run workflow`

默认时间：
- `cron: "30 0 * * *"`
- 这是 UTC 时间，对应北京时间每天 `08:30`

使用前需要：
- 把项目上传到 GitHub 仓库
- 在仓库 `Settings -> Secrets and variables -> Actions` 中配置这些 `Secrets`

建议配置的 Secrets：
- `FEISHU_WEBHOOK_URL`
- `FEISHU_BOT_SECRET`
- `OPENAI_API_KEY`
- `OPENAI_BASE_URL`
- `OPENAI_MODEL`
- `LOOKBACK_HOURS`
- `MAX_NEWS_ITEMS`
- `RSS_FETCH_TIMEOUT`
- `RSS_FETCH_WORKERS`
- `TITLE_SIMILARITY_THRESHOLD`

说明：
- 如果某些可选变量不配，GitHub Actions 中对应环境变量会为空字符串
- `FEISHU_WEBHOOK_URL` 和 `OPENAI_API_KEY` 通常是必须的
