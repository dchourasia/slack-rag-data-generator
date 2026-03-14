# Slack Channel History Extraction Pipeline

Extract, sanitize, and chunk an entire Slack channel's history for LLM ingestion. Opt-out requests are collected via a Google Form and applied automatically before each run.

## Prerequisites

- Python 3.10+
- A Slack Bot OAuth token with the following scopes:
  - `channels:history`
  - `channels:read`
  - `users:read`
  - `users:read.email`
- A Google Cloud service account with the Google Forms API enabled
- The bot must be invited to the target Slack channel

## Setup

1. **Install dependencies**

   ```bash
   pip install -r requirements.txt
   ```

2. **Configure secrets in `.env`**

   ```
   SLACK_BOT_TOKEN=xoxb-your-real-token
   GOOGLE_SERVICE_ACCOUNT_KEY_FILE=service-account-key.json
   ```

3. **Set the target Slack channel**

   Edit `config.yaml` and set:
   - `slack.channel_id` -- the Slack channel ID (e.g. `C06XXXXXXX`)
   - `slack.workspace_url` -- your workspace URL (e.g. `https://myteam.slack.com`)

4. **Set up Google Form integration**

   a. Create a Google Cloud project and enable the **Google Forms API**.

   b. Create a **service account** in the project and download its JSON key file.

   c. **Share the Google Form** with the service account's email address
      (e.g. `my-sa@my-project.iam.gserviceaccount.com`) so it can read responses.

   d. Find the question IDs by running:

      ```bash
      python main.py --step form-info
      ```

      This prints each question's title and its `questionId`.

   e. Edit `config.yaml` and fill in the `google_form` section:
      - `form_id` -- the Google Form ID (from the form URL)
      - `user_id_question_id` -- question ID for the "user ID to exclude" question
      - `message_links_question_id` -- question ID for the "message links" question

5. **Define manual exclusion rules** (optional)

   Edit `exclusion_rules.yaml` to list additional entries beyond what the Google Form provides:
   - `excluded_users` -- Slack user IDs whose messages should be removed
   - `excluded_message_links` -- specific message permalinks to remove

## Usage

### Full pipeline

```bash
python main.py
```

This runs all four steps in sequence:
1. **Form Sync** -- fetches opt-out responses from the Google Form and merges them into `exclusion_rules.yaml`
2. **Extract** -- downloads every message and thread reply from the channel
3. **Sanitize** -- applies exclusion rules, anonymizes user mentions
4. **Chunk** -- splits the cleaned text into token-bounded files

To skip the Google Form sync (e.g. when running offline):

```bash
python main.py --skip-form-sync
```

### Run individual steps

```bash
# Sync exclusion rules from Google Form only
python main.py --step form-sync

# Print Google Form question IDs (setup helper)
python main.py --step form-info

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
| `slack_output_sources_<timestamp>/` | `chunk_001.txt`, `chunk_002.txt`, ... and `chunk_summary.json` |

## Configuration Reference

### config.yaml

| Key | Description | Default |
|---|---|---|
| `slack.channel_id` | Target Slack channel ID | -- |
| `slack.workspace_url` | Workspace URL for constructing message links | -- |
| `google_form.form_id` | Google Form ID | -- |
| `google_form.user_id_question_id` | Question ID for user IDs to exclude | -- |
| `google_form.message_links_question_id` | Question ID for message links to exclude | -- |
| `output.base_dir` | Parent directory for output folders | `.` |
| `chunking.max_tokens_per_chunk` | Maximum tokens per chunk file | `50000` |
| `chunking.chunk_overlap` | Token overlap between chunks | `200` |
| `chunking.encoding_name` | tiktoken encoding name | `cl100k_base` |

### .env

| Variable | Description |
|---|---|
| `SLACK_BOT_TOKEN` | Slack Bot User OAuth Token |
| `GOOGLE_SERVICE_ACCOUNT_KEY_FILE` | Path to Google service account JSON key file |

### exclusion_rules.yaml

| Key | Description |
|---|---|
| `excluded_users` | List of Slack user IDs to exclude (manual + form-sourced) |
| `excluded_message_links` | List of message permalinks to exclude (manual + form-sourced) |
