---
name: Slack History Extraction Pipeline
overview: Build a Python pipeline that extracts a Slack channel's full chat history (messages + thread replies), sanitizes the data by anonymizing users and applying exclusion rules, and splits the cleaned text into token-bounded chunk files for LLM consumption.
todos:
  - id: config-files
    content: Create .env, config.yaml, and exclusion_rules.yaml configuration files
    status: completed
  - id: requirements
    content: Create requirements.txt with slack-sdk, python-dotenv, PyYAML, langchain-text-splitters, tiktoken
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
    content: "Implement main.py: CLI orchestrator that chains extract -> sanitize -> chunk with logging and step-selection support"
    status: completed
  - id: readme
    content: Create README.md with setup instructions, Slack bot OAuth scope requirements, and usage examples
    status: completed
isProject: false
---

# Slack Channel History Extraction, Sanitization, and Chunking Pipeline

## Project Structure

All files will be created in the workspace root: `/home/dchouras/RHODS/DevOps/AI/DevTestOps NotebookLM/`

```
.
├── .env                      # Secrets (Slack bot token) -- gitignored
├── config.yaml               # Main configuration
├── exclusion_rules.yaml      # User and message-link exclusion rules
├── requirements.txt          # Python dependencies
├── slack_extractor.py        # Step 1: Raw data extraction
├── data_sanitizer.py         # Step 2: Data cleanup and anonymization
├── data_chunker.py           # Step 3: Token-based splitting
├── main.py                   # Orchestrator (runs all 3 steps sequentially)
└── README.md                 # Setup and usage instructions
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
```

The bot token requires these OAuth scopes: `channels:history`, `channels:read`, `users:read`, `users:read.email`.

### 1b. `config.yaml` -- Main Configuration

```yaml
slack:
  channel_id: "C0XXXXXXX"         # Target channel ID
  workspace_url: "https://yourworkspace.slack.com"  # For constructing message links

output:
  base_dir: "."                    # Parent directory for all output folders

chunking:
  max_tokens_per_chunk: 50000     # Max tokens per output chunk file
  chunk_overlap: 200              # Token overlap between consecutive chunks
  encoding_name: "cl100k_base"   # tiktoken encoding (GPT-4 / Claude compatible)
```

### 1c. `exclusion_rules.yaml` -- Exclusion Rules

```yaml
excluded_users:
  - "U12345ABCDE"    # Slack user IDs to exclude
  - "U67890FGHIJ"

excluded_message_links:
  - "https://yourworkspace.slack.com/archives/C0XXXXXXX/p1234567890123456"
```

---

## 2. Dependencies (`requirements.txt`)

```
slack-sdk>=3.27.0
python-dotenv>=1.0.0
PyYAML>=6.0
langchain-text-splitters>=0.2.0
tiktoken>=0.7.0
```

---

## 3. Step 1 -- Raw Data Extraction (`slack_extractor.py`)

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

## 4. Step 2 -- Data Sanitization (`data_sanitizer.py`)

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

## 5. Step 3 -- Token-Based Chunking (`data_chunker.py`)

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

## 6. Orchestrator (`main.py`)

Single entry point that runs all three steps sequentially, passing each step's output directory as input to the next:

```python
raw_dir = extract_slack_data(config)              # Step 1
processed_dir = sanitize_data(raw_dir, config)    # Step 2
output_dir = chunk_data(processed_dir, config)    # Step 3
```

Supports CLI arguments to run individual steps or the full pipeline:

```bash
python main.py                          # Full pipeline
python main.py --step extract           # Only extraction
python main.py --step sanitize --input slack_raw_data_20260314_120000
python main.py --step chunk --input slack_processed_data_20260314_120000
```

Includes logging to both console and a log file with timestamps.

---

## 7. Cross-Cutting Concerns

- **Logging**: All scripts use Python `logging` module with INFO level to console and DEBUG to a log file. Progress indicators for long-running extraction (e.g., "Fetched 500/2340 messages...").
- **Error Handling**: Graceful handling of API errors, missing config values, and file I/O errors. The extraction step saves progress periodically so a crash doesn't lose all work.
- **Timestamps**: All directory suffixes use format `YYYYMMDD_HHMMSS` from `datetime.now().strftime("%Y%m%d_%H%M%S")`.
- **README.md**: Documents prerequisites (Python 3.10+, Slack bot setup with required scopes), installation steps (`pip install -r requirements.txt`), configuration instructions, and usage examples.

