---
name: Slack History Extraction Pipeline
overview: Build a Python pipeline that fetches opt-out requests from a Google Form, extracts a Slack channel's full chat history (messages + thread replies), sanitizes the data by anonymizing users and applying exclusion rules, and splits the cleaned text into token-bounded chunk files for LLM consumption.
todos:
  - id: config-files
    content: Create .env, config.yaml, and exclusion_rules.yaml configuration files
    status: completed
  - id: requirements
    content: Create requirements.txt with slack-sdk, python-dotenv, PyYAML, langchain-text-splitters, tiktoken, google-api-python-client, google-auth
    status: completed
  - id: form-extractor
    content: "Implement google_form_extractor.py: GoogleFormExtractor class with service account auth, paginated response fetching, rate-limit retry, response parsing, and exclusion_rules.yaml merge"
    status: completed
  - id: extractor
    content: "Implement slack_extractor.py: full channel history + thread replies extraction with pagination, rate limiting, user map, and message link construction"
    status: completed
  - id: sanitizer
    content: "Implement data_sanitizer.py: exclusion filtering (by author, mentions, links), user anonymization (@userN), and output to single text file"
    status: completed
  - id: chunker
    content: "Implement data_chunker.py: token-based splitting using langchain TokenTextSplitter with 50000-token chunks"
    status: completed
  - id: orchestrator
    content: "Implement main.py: CLI orchestrator that chains form-sync -> extract -> sanitize -> chunk with logging and step-selection support"
    status: completed
  - id: readme
    content: Create README.md with setup instructions, Google Form setup, Slack bot OAuth scope requirements, and usage examples
    status: completed
isProject: false
---

# Slack Channel History Extraction, Sanitization, and Chunking Pipeline

## Project Structure

All files live in the workspace root: `/home/dchouras/RHODS/DevOps/AI/DevTestOps NotebookLM/`

```
.
├── .env                         # Secrets (Slack bot token, Google SA key path)
├── config.yaml                  # Main configuration
├── exclusion_rules.yaml         # User and message-link exclusion rules
├── requirements.txt             # Python dependencies
├── google_form_extractor.py     # Step 1: Google Form opt-out sync
├── slack_extractor.py           # Step 2: Raw data extraction
├── data_sanitizer.py            # Step 3: Data cleanup and anonymization
├── data_chunker.py              # Step 4: Token-based splitting
├── main.py                      # Orchestrator (runs all 4 steps sequentially)
└── README.md                    # Setup and usage instructions
```

Output directories (auto-created at runtime):

- `slack_raw_data_<YYYYMMDD_HHMMSS>/` -- raw JSON files
- `slack_processed_data_<YYYYMMDD_HHMMSS>/` -- sanitized text
- `slack_output_sources_<YYYYMMDD_HHMMSS>/` -- chunked text files

---

## 1. Configuration Files

### 1a. `.env` -- Secrets

```env
SLACK_BOT_TOKEN=xoxb-your-bot-token-here
GOOGLE_SERVICE_ACCOUNT_KEY_FILE=service-account-key.json
```

- The Slack bot token requires these OAuth scopes: `channels:history`, `channels:read`, `users:read`, `users:read.email`.
- The Google service account key file must be a JSON key downloaded from the Google Cloud console. The Google Form must be shared with the service account's email address.

### 1b. `config.yaml` -- Main Configuration

```yaml
slack:
  channel_id: "C0XXXXXXX"
  workspace_url: "https://yourworkspace.slack.com"

google_form:
  form_id: "1FAIpQLSe_XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
  user_id_question_id: "6f1a2b3c"
  message_links_question_id: "7d4e5f6a"

output:
  base_dir: "."

chunking:
  max_tokens_per_chunk: 50000
  chunk_overlap: 200
  encoding_name: "cl100k_base"
```

- `google_form.form_id` -- the Google Form ID (from the URL `https://docs.google.com/forms/d/<FORM_ID>/...`).
- `google_form.user_id_question_id` / `message_links_question_id` -- the internal question IDs. Use `python main.py --step form-info` to discover them.

### 1c. `exclusion_rules.yaml` -- Exclusion Rules

```yaml
excluded_users:
  - "U12345ABCDE"
  - "U67890FGHIJ"

excluded_message_links:
  - "https://yourworkspace.slack.com/archives/C0XXXXXXX/p1234567890123456"
```

This file can contain manually-added entries. The Google Form sync step merges form-sourced entries into this file (deduplicating automatically) before each pipeline run.

---

## 2. Dependencies (`requirements.txt`)

```
slack-sdk>=3.27.0
python-dotenv>=1.0.0
PyYAML>=6.0
langchain-text-splitters>=0.2.0
tiktoken>=0.7.0
google-api-python-client>=2.130.0
google-auth>=2.29.0
```

---

## 3. Step 1 -- Google Form Opt-Out Sync (`google_form_extractor.py`)

### Background

A Google Form lets users submit opt-out requests. The form has two optional questions:
- **Question 1**: A single Slack user ID to exclude (at most one per response).
- **Question 2**: One or more Slack message links to exclude (newline-separated).

Before extracting Slack data, this step fetches all form responses and merges them into `exclusion_rules.yaml`.

### Data Flow

```mermaid
flowchart TD
    Start[Start] --> Auth["Authenticate via service account key file"]
    Auth --> FetchResponses["Call forms.responses.list with pagination"]
    FetchResponses --> CheckPage{nextPageToken?}
    CheckPage -->|Yes| FetchResponses
    CheckPage -->|No| ParseAll[Parse all collected responses]
    ParseAll --> ParseQ1["Q1: Extract user ID, skip empty"]
    ParseAll --> ParseQ2["Q2: Extract message links, split by newline, skip empty"]
    ParseQ1 --> LoadYAML["Load existing exclusion_rules.yaml"]
    ParseQ2 --> LoadYAML
    LoadYAML --> Merge["Union of existing + form-sourced entries, deduplicate"]
    Merge --> WriteYAML["Write updated exclusion_rules.yaml"]
    WriteYAML --> Done["Log summary and return"]
```

### Key Implementation Details

- **Class**: `GoogleFormExtractor` with methods `fetch_and_update_exclusion_rules()` (main entry) and `print_form_structure()` (setup helper).
- **Authentication**: Uses `google.oauth2.service_account.Credentials.from_service_account_file()` with the key file path from `.env`, scoped to `https://www.googleapis.com/auth/forms.responses.readonly`. Builds the Forms v1 service via `googleapiclient.discovery.build("forms", "v1", credentials=creds)`.
- **Pagination**: Uses `list_next()` from the Google API client library to iterate through all pages of responses:
  ```python
  request = service.forms().responses().list(formId=form_id)
  while request is not None:
      result = request.execute()
      all_responses.extend(result.get("responses", []))
      request = service.forms().responses().list_next(request, result)
  ```
- **Rate Limiting**: Wraps every Google API call in a retry helper. On HTTP 429, 500, or 503 responses, reads the `retry-after` header and retries up to 5 times with exponential backoff (minimum 1 second).
- **Response Parsing**: Each form response has an `answers` dict keyed by question ID. Question 1 yields a single `textAnswers.answers[0].value` (user ID). Question 2 yields a multi-line string that is split on newlines. Empty values are skipped.
- **Merge Logic**: Reads the existing `exclusion_rules.yaml`, takes the set union of old + new entries for both `excluded_users` and `excluded_message_links`, deduplicates, sorts for stable output, and writes back.
- **`print_form_structure()`**: Calls `forms.get()` (requires scope `forms.body.readonly`) and prints each question's title alongside its `questionId`. Used during initial setup to populate `config.yaml`.

---

## 4. Step 2 -- Raw Data Extraction (`slack_extractor.py`)

### Data Flow

```mermaid
flowchart TD
    Start[Start] --> LoadConfig[Load config.yaml and .env]
    LoadConfig --> FetchUsers[Fetch all workspace users via users.list API]
    FetchUsers --> BuildUserMap["Build user_id -> display_name map"]
    BuildUserMap --> FetchHistory["Call conversations.history with pagination"]
    FetchHistory --> CheckCursor{next_cursor exists?}
    CheckCursor -->|Yes| FetchHistory
    CheckCursor -->|No| ProcessMessages[Iterate over all messages]
    ProcessMessages --> HasThread{"reply_count > 0?"}
    HasThread -->|Yes| FetchReplies["Call conversations.replies with pagination"]
    HasThread -->|No| BuildRecord[Build message record]
    FetchReplies --> BuildRecord
    BuildRecord --> MoreMessages{More messages?}
    MoreMessages -->|Yes| ProcessMessages
    MoreMessages -->|No| SaveJSON["Save to slack_raw_data_<ts>/messages.json"]
    SaveJSON --> SaveMeta["Save metadata.json"]
    SaveMeta --> Done[Return output directory path]
```

### Key Implementation Details

- **Slack SDK Client**: Use `slack_sdk.WebClient(token=bot_token)` -- do NOT use the MCP Slack tools (they lack pagination cursor support needed for full extraction).
- **Pagination**: Every paginated API call (`conversations.history`, `conversations.replies`, `users.list`) must loop until `response_metadata.next_cursor` is empty/absent:

```python
  cursor = None
  while True:
      response = client.conversations_history(channel=channel_id, cursor=cursor, limit=200)
      messages.extend(response["messages"])
      cursor = response.get("response_metadata", {}).get("next_cursor")
      if not cursor:
          break
```

- **Rate Limiting (HTTP 429)**: Wrap every API call in a retry helper that catches `SlackApiError`. When `response.status_code == 429`, read the `Retry-After` header (seconds) and `time.sleep()` that duration (minimum 1 second fallback). Retry up to 5 times with exponential backoff.
- **User Map**: Before extracting messages, fetch all users via `users.list` (paginated) to build a `dict[str, str]` mapping `user_id` -> `real_name` or `display_name`. This map is saved alongside the raw data for reference.
- **Message Link Construction**: Build links locally instead of calling `chat.getPermalink` (avoids extra rate-limited API calls):
  - Top-level message: `{workspace_url}/archives/{channel_id}/p{ts.replace('.', '')}`
  - Thread reply: `{workspace_url}/archives/{channel_id}/p{reply_ts.replace('.', '')}?thread_ts={parent_ts.replace('.', '')}&cid={channel_id}`
- **Raw Data Schema** (`messages.json`): A JSON array where each element is:

```json
  {
    "ts": "1710000000.000100",
    "user_id": "U12345ABC",
    "user_name": "john.doe",
    "text": "Hello <@U67890DEF> please review this",
    "link": "https://workspace.slack.com/archives/C0XXX/p1710000000000100",
    "thread_ts": "1710000000.000100",
    "reply_count": 2,
    "replies": [
      {
        "ts": "1710000001.000200",
        "user_id": "U67890DEF",
        "user_name": "jane.smith",
        "text": "Sure, looking at it now",
        "link": "https://workspace.slack.com/archives/C0XXX/p1710000001000200?thread_ts=1710000000000100&cid=C0XXX"
      }
    ]
  }
```

- **Output**: `metadata.json` alongside `messages.json` records the channel ID, extraction timestamp, total message count, and the user map.

---

## 5. Step 3 -- Data Sanitization (`data_sanitizer.py`)

### Data Flow

```mermaid
flowchart TD
    Start[Start] --> LoadRaw["Load messages.json from raw data dir"]
    LoadRaw --> LoadExclusion[Load exclusion_rules.yaml]
    LoadExclusion --> BuildAnonMap["Build user_id -> @userN anonymization map"]
    BuildAnonMap --> IterateMsg[For each message + its replies]
    IterateMsg --> CheckAuthor{"Author in excluded_users?"}
    CheckAuthor -->|Yes| Skip[Skip message]
    CheckAuthor -->|No| CheckMentions{"Any <@excluded_user> in text?"}
    CheckMentions -->|Yes| Skip
    CheckMentions -->|No| CheckLink{"Message link in excluded_message_links?"}
    CheckLink -->|Yes| Skip
    CheckLink -->|No| Anonymize["Replace all <@UXXXX> with @userN"]
    Anonymize --> AppendOutput[Append to output text]
    Skip --> NextMsg{More messages?}
    AppendOutput --> NextMsg
    NextMsg -->|Yes| IterateMsg
    NextMsg -->|No| WriteFile["Write all_messages.txt to slack_processed_data_<ts>/"]
    WriteFile --> SaveMap["Save anonymization_map.json"]
    SaveMap --> Done[Return output directory path]
```

### Key Implementation Details

- **Anonymization Map**: Scan all messages (including replies) to collect every unique `user_id`. Assign sequential dummy names: `@user1`, `@user2`, ... Store the mapping in `anonymization_map.json` for traceability (maps dummy name back to original user_id).
- **Exclusion Logic** (applied to every message AND every reply independently):
  1. **Author exclusion**: If `message.user_id` is in `excluded_users` list, skip the entire message.
  2. **Mention exclusion**: Parse all `<@UXXXXXXX>` patterns in the message text. If ANY matched user_id is in `excluded_users`, skip the message.
  3. **Link exclusion**: If `message.link` is in `excluded_message_links`, skip the message.
- **User Mention Replacement**: After exclusion checks pass, use regex `r'<@(U[A-Z0-9]+)>'` to find all user mentions in the text and replace each with the corresponding `@userN` from the anonymization map.
- **Output Format** (`all_messages.txt`): One message per logical block, ordered chronologically. Thread replies indented under their parent:

```
  [2024-03-10 14:30:00] @user1: Hello @user2 please review this
    [2024-03-10 14:31:00] @user2: Sure, looking at it now
    [2024-03-10 14:35:00] @user3: Done, LGTM
  [2024-03-10 15:00:00] @user4: Deployment complete
```

  Timestamps derived from Slack `ts` field (Unix epoch -> human-readable UTC).

---

## 6. Step 4 -- Token-Based Chunking (`data_chunker.py`)

### Data Flow

```mermaid
flowchart TD
    Start[Start] --> LoadText["Read all_messages.txt from processed data dir"]
    LoadText --> InitSplitter["Initialize TokenTextSplitter with chunk_size=50000, encoding=cl100k_base"]
    InitSplitter --> SplitText["Split text into token-bounded chunks"]
    SplitText --> WriteChunks["Write each chunk to chunk_001.txt, chunk_002.txt, ..."]
    WriteChunks --> WriteSummary["Write chunk_summary.json with chunk count and token counts"]
    WriteSummary --> Done[Return output directory path]
```

### Key Implementation Details

- **Splitter Configuration**:

```python
  from langchain_text_splitters import TokenTextSplitter

  splitter = TokenTextSplitter(
      encoding_name="cl100k_base",   # from config
      chunk_size=50000,               # from config
      chunk_overlap=200,              # from config -- prevents cutting mid-context
  )
  chunks = splitter.split_text(full_text)
```

- **Output Files**: Each chunk written as `chunk_001.txt`, `chunk_002.txt`, etc. inside `slack_output_sources_<YYYYMMDD_HHMMSS>/`.
- **Summary Metadata**: `chunk_summary.json` records total chunks, tokens per chunk, and the source processed-data directory for traceability.

---

## 7. Orchestrator (`main.py`)

Single entry point that runs all four steps sequentially, passing each step's output directory as input to the next:

```python
form_extractor.fetch_and_update_exclusion_rules()  # Step 1
raw_dir = extract_slack_data(config)                # Step 2
processed_dir = sanitize_data(raw_dir, config)      # Step 3
output_dir = chunk_data(processed_dir, config)      # Step 4
```

Supports CLI arguments to run individual steps or the full pipeline:

```bash
python main.py                          # Full pipeline (all 4 steps)
python main.py --skip-form-sync         # Full pipeline, skip Google Form sync
python main.py --step form-sync         # Only sync exclusion rules from Google Form
python main.py --step form-info         # Print form question IDs (setup helper)
python main.py --step extract           # Only Slack extraction
python main.py --step sanitize --input slack_raw_data_20260314_120000
python main.py --step chunk --input slack_processed_data_20260314_120000
```

Includes logging to both console and a log file with timestamps.

---

## 8. End-to-End Pipeline Flow

```mermaid
flowchart LR
    FormSync["Step 1: Form Sync"] -->|"updates exclusion_rules.yaml"| Extract["Step 2: Extract"]
    Extract -->|"slack_raw_data_<ts>/"| Sanitize["Step 3: Sanitize"]
    Sanitize -->|"slack_processed_data_<ts>/"| Chunk["Step 4: Chunk"]
    Chunk -->|"slack_output_sources_<ts>/"| Done["LLM-ready chunks"]
```

---

## 9. Cross-Cutting Concerns

- **Logging**: All scripts use Python `logging` module with INFO level to console and DEBUG to a log file. Progress indicators for long-running extraction (e.g., "Fetched 500/2340 messages...").
- **Error Handling**: Graceful handling of API errors, missing config values, and file I/O errors. The extraction step saves progress periodically so a crash doesn't lose all work.
- **Rate Limiting**: Both Slack and Google API calls are wrapped in retry helpers that honor `Retry-After` / `retry-after` headers and fall back to exponential backoff.
- **Timestamps**: All directory suffixes use format `YYYYMMDD_HHMMSS` from `datetime.now().strftime("%Y%m%d_%H%M%S")`.
- **README.md**: Documents prerequisites (Python 3.10+, Slack bot setup, Google Cloud service account, Google Form sharing), installation steps, configuration instructions, and usage examples.
