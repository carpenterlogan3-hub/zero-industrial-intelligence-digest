# Zero Industrial — Daily Intelligence Digest Pipeline

Automated daily intelligence briefing system for Zero Industrial Inc. Scrapes 19 public RSS feeds, classifies articles using LangChain + OpenAI GPT-4o-mini, generates role-tailored HTML digests, and delivers to 4 stakeholders via Gmail API and Slack.

Built by Logan Carpenter | Griz, Inc. | AI Systems Integration

## Architecture

```
RSS Feeds (19) → Google Sheets (Raw) → LangChain/GPT-4o-mini (Classify) → Google Sheets (Classified) → GPT-4o-mini (Generate) → Gmail/Slack (Deliver)
```

**Pipeline stages (run sequentially, daily 6:00 AM ET):**

| Stage | Module directory | What it does |
|-------|-----------------|--------------|
| BR_01 | `src/br01/` | Fetch 19 RSS feeds, 24hr lookback, deduplicate, store raw articles |
| BR_02 | `src/br02/` | Classify each article: topic, roles, importance, summary via LangChain |
| BR_03 | `src/br03/` | Generate HTML digests per stakeholder, send via Gmail + Slack |
| BR_04 | `src/br04/` | Log errors to Sheets, send admin completion summary |

**Stakeholders (all daily cadence):**

| Name | Role | Email (placeholder) |
|------|------|-------------------|
| Ted Kniesche | Founder & CEO | CruisecontrolcargoLLC@gmail.com |
| Michael Brady | SVP Development - Canada | LoganCarpenter99@icloud.com |
| William Price | VP Finance & Accounting | Carpenterlogan3@gmail.com |
| Logan Carpenter | AI/IT Admin | logan@grizinc.com + Slack #digest-ai |

## Setup

### 1. Clone and install
```bash
git clone https://github.com/YOUR_USERNAME/zero-industrial-intelligence-digest.git
cd zero-industrial-intelligence-digest
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure environment
```bash
cp .env.example .env
# Edit .env with your actual credentials
```

### 3. Google Sheets setup
Create a Google Sheet with 3 tabs:
- **Raw Feed Items** — headers: date_pulled, source, title, url, summary, pub_date, feed_category, processed
- **Classified Items** — headers: title, url, source, pub_date, topic_category, relevant_roles, importance, one_line_summary, digest_date
- **Errors** — headers: timestamp, pipeline_run_date, module_name, error_type, error_message, affected_item

Share the spreadsheet with your service account email as Editor.

### 4. Run locally
```bash
python -m src.main
```

### 5. Deploy to GitHub Actions
Add these secrets in GitHub → Settings → Secrets:
- `OPENAI_API_KEY`
- `SPREADSHEET_ID`
- `SLACK_BOT_TOKEN`

Upload `service_account.json` as a repository secret or use a GitHub Action to write it from a secret.

The pipeline runs automatically at 6:00 AM ET daily via `.github/workflows/daily_pipeline.yml`.

## Documentation

- **Requirements & Exceptions**: `V2__Requirements_and_Exceptions.xlsx` (Tab 2: Intelligence Briefing Document)
- **Stakeholder Config**: Same workbook, Tab 3: Stakeholder Distribution Config
- **Process Flow Diagrams**: Generated in Claude.ai conversation alongside the spreadsheet

Every `.py` file in `src/` maps 1:1 to a row in the spreadsheet's column A (ID). The docstring in each file mirrors the spreadsheet's Usecase, Expected Input, Expected Output, and Exception columns.

## Tech Stack

- **Python 3.9+** — runtime
- **LangChain + langchain-openai** — LLM orchestration
- **OpenAI GPT-4o-mini** — classification (temp=0.2) + digest generation (temp=0.4)
- **feedparser** — RSS ingestion
- **gspread + google-auth** — Google Sheets data layer
- **Gmail API** — email delivery (primary)
- **slack-sdk** — Slack delivery (secondary, Logan only)
- **GitHub Actions** — daily cron trigger (0 10 * * * = 6:00 AM ET)
