# Slack Channel History Extraction Pipeline

Extract, sanitize, and chunk an entire Slack channel's history for LLM ingestion.

## Prerequisites

- Python 3.10+
- A Slack Bot OAuth token with the following scopes:
  - `channels:history`
  - `channels:read`
  - `users:read`
  - `users:read.email`

The bot must be invited to the target channel.

## Setup

1. **Install dependencies**

   ```bash
   pip install -r requirements.txt
   ```

2. **Configure the bot token**

   Copy the placeholder `.env` file and fill in your real token:

   ```
   SLACK_BOT_TOKEN=xoxb-your-real-token
   ```

3. **Set the target channel**

   Edit `config.yaml` and set:
   - `slack.channel_id` – the Slack channel ID (e.g. `C06XXXXXXX`)
   - `slack.workspace_url` – your workspace URL (e.g. `https://myteam.slack.com`)

4. **Define exclusion rules** (optional)

   Edit `exclusion_rules.yaml` to list:
   - `excluded_users` – Slack user IDs whose messages should be removed
   - `excluded_message_links` – specific message permalinks to remove

## Usage

### Full pipeline

```bash
python main.py
```

This runs all three steps in sequence:
1. **Extract** – downloads every message and thread reply from the channel
2. **Sanitize** – applies exclusion rules, anonymizes user mentions
3. **Chunk** – splits the cleaned text into token-bounded files

### Run individual steps

```bash
# Extract only
python main.py --step extract

# Sanitize a previously extracted dataset
python main.py --step sanitize --input slack_raw_data_20260314_120000

# Chunk a previously sanitized dataset
python main.py --step chunk --input slack_processed_data_20260314_120000
```

## Output Directories

| Directory | Contents |
|---|---|
| `slack_raw_data_<timestamp>/` | `messages.json` (structured messages + replies) and `metadata.json` |
| `slack_processed_data_<timestamp>/` | `all_messages.txt` (anonymized plain text) and `anonymization_map.json` |
| `slack_output_sources_<timestamp>/` | `chunk_001.txt`, `chunk_002.txt`, … and `chunk_summary.json` |

## Configuration Reference

### config.yaml

| Key | Description | Default |
|---|---|---|
| `slack.channel_id` | Target Slack channel ID | – |
| `slack.workspace_url` | Workspace URL for constructing message links | – |
| `output.base_dir` | Parent directory for output folders | `.` |
| `chunking.max_tokens_per_chunk` | Maximum tokens per chunk file | `50000` |
| `chunking.chunk_overlap` | Token overlap between chunks | `200` |
| `chunking.encoding_name` | tiktoken encoding name | `cl100k_base` |

### exclusion_rules.yaml

| Key | Description |
|---|---|
| `excluded_users` | List of Slack user IDs to exclude |
| `excluded_message_links` | List of message permalinks to exclude |
