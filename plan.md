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

All files live in the workspace root: `/Users/dchouras/RHODS/DevOps/slack-rag-data-generator/`

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
SLACK_USER_TOKEN=xoxp-your-user-token-here
GOOGLE_SERVICE_ACCOUNT_KEY_FILE=service-account-key.json
```

- The Slack **bot token** requires these OAuth scopes: `channels:history`, `channels:read`, `users:read`, `users:read.email`, `lists:read`.
- The Slack **user token** (optional, for list item comment extraction) requires these User Token Scopes: `channels:history`, `groups:history`, `lists:read`. If not set, the pipeline still runs but skips list item comment extraction.
- The `lists:read` scope enables fetching list item details (title, status, assignee, dates, notes) via `slackLists.items.list`. If missing, comments are still extracted but without correlated item details.
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
    HasThread -->|No| DetectList{"Text contains list URL?"}
    FetchReplies --> DetectList
    DetectList -->|Yes| CollectListID["Collect list_id + record_id"]
    DetectList -->|No| BuildRecord[Build message record]
    CollectListID --> BuildRecord
    BuildRecord --> MoreMessages{More messages?}
    MoreMessages -->|Yes| ProcessMessages
    MoreMessages -->|No| HasUserToken{"SLACK_USER_TOKEN set?"}
    HasUserToken -->|Yes| FetchListComments["For each unique list_id:\n1. Derive conversation C... from F...\n2. conversations.history on list conv\n3. conversations.replies for each threaded item"]
    HasUserToken -->|No| SkipLists["Log warning, skip list comments"]
    FetchListComments --> FetchListItems["For each unique list_id:\nCall slackLists.items.list\n(bot token, fallback to user token)"]
    SkipLists --> FetchListItems
    FetchListItems --> Correlate["Correlate items with comments\n(match date_created ↔ item_ts)"]
    Correlate --> SaveJSON["Save messages.json + list_comments.json + list_data.json"]
    SaveJSON --> SaveMeta["Save metadata.json"]
    SaveMeta --> Done[Return output directory path]
```

### Key Implementation Details

- **Two Slack Clients**: A bot client (`SLACK_BOT_TOKEN`, `xoxb-`) for channel messages and user data. An optional user client (`SLACK_USER_TOKEN`, `xoxp-`) for list item comment access. If the user token is absent, list comment extraction is skipped gracefully.
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

- **Rate Limiting & Resilient Retries**: Wrap every API call in `_call_with_retry`, which handles three error categories up to 5 retries:
  1. **HTTP 429 (rate-limit)**: Catches `SlackApiError` with `status_code == 429`, reads the `Retry-After` header and sleeps that duration (minimum 1 second fallback).
  2. **Network errors**: Catches `ConnectionError`, `TimeoutError`, and `OSError` and retries with exponential backoff (`1s, 2s, 4s, …`).
  3. **Incomplete responses**: Catches any exception whose type name or message contains `IncompleteRead` (HTTP chunked-transfer failures) and retries with the same exponential backoff.
- **User Map**: Before extracting messages, fetch all users via `users.list` (paginated) to build a `dict[str, str]` mapping `user_id` -> `real_name` or `display_name`. This map is saved alongside the raw data for reference.
- **Message Link Construction**: Build links locally instead of calling `chat.getPermalink` (avoids extra rate-limited API calls):
  - Top-level message: `{workspace_url}/archives/{channel_id}/p{ts.replace('.', '')}`
  - Thread reply: `{workspace_url}/archives/{channel_id}/p{reply_ts.replace('.', '')}?thread_ts={parent_ts.replace('.', '')}&cid={channel_id}`
- **List Item Detection**: Each message's text is scanned with regex for Slack List URLs matching `slack.com/lists/<team>/<list_id>?record_id=<record_id>`. Messages containing these URLs are tagged with `is_list_item: true` and their `list_refs` (list_id + record_id pairs).
- **List Item Comment Extraction** (user token required): Slack stores list item comments in a hidden companion conversation. Each List file (`F07SBP17R7Z`) has a parallel conversation (`C07SBP17R7Z`) where every list item is a message and comments are thread replies. The extractor:
  1. Collects all unique list IDs from channel messages.
  2. For each list, derives the conversation ID by swapping the `F` prefix for `C`.
  3. Calls `conversations.history` on the list conversation (user token) to get all item messages.
  4. Calls `conversations.replies` for each item with `reply_count > 0` to fetch comments.
  5. Saves the results in `list_comments.json` keyed by list ID.
  6. If the user token cannot access a list conversation, logs a warning and skips that list.
- **Record ID Extraction from Hidden Conversation Messages**: Each item message in the hidden conversation is inspected by `_extract_record_id_from_message()` to extract the `record_id` it represents. The function tries three strategies in order:
  1. Parse the message's `blocks` → `elements` → child elements, looking for URLs containing the list ID and extracting the `record_id=RecXXX` query parameter, or for text containing a `RecXXX` pattern.
  2. Check `metadata.event_payload` for a `record_id` or `item_id` key.
  3. Regex-search the message's plain `text` for `record_id=RecXXX`.
  The extracted record_id is stored alongside each comment group so it can later be used for precise correlation with list item details.
- **List Item Detail Extraction** (requires `lists:read` scope): After fetching comments, the extractor calls `slackLists.items.list` for each list to retrieve structured item details (title, status, owner, date, notes, etc.). The process:
  1. Tries the bot token first; falls back to the user token if the bot lacks `lists:read`.
  2. Fetches **both active and archived items** (two paginated passes: default + `archived=true`). Archived items are items that were deleted or completed and removed from the active list but still have comments in the hidden conversation.
  3. Parses each item's `fields` array into a flat key-value dict (text, select, date, user, number, etc.).
  4. Uses the first text field as the item title.
  5. Correlates items with hidden conversation comments using **fuzzy timestamp matching** (±5 second window). The Lists API `date_created` and the hidden conversation `item_ts` represent the same creation event but are recorded 1-3 seconds apart. The algorithm finds the closest timestamp match within the window for each item.
  6. Unmatched comment groups (where no item detail could be correlated, e.g. system events) are preserved separately.
  7. Saves the merged result in `list_data.json` keyed by list ID, containing `items` (with details + matched comments) and `unmatched_comments`.
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
    "is_list_item": false,
    "list_refs": [],
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

- **List Comments Schema** (`list_comments.json`): A dict keyed by list ID, each containing an array of items with comments:

```json
  {
    "F07SBP17R7Z": [
      {
        "list_id": "F07SBP17R7Z",
        "conversation_id": "C07SBP17R7Z",
        "item_ts": "1710000000.000100",
        "item_text": "A comment was added",
        "comments": [
          {
            "ts": "1710000001.000200",
            "user_id": "U67890DEF",
            "user_name": "jane.smith",
            "text": "Looking into this issue now",
            "link": "https://workspace.slack.com/archives/C07SBP17R7Z/p1710000001000200?..."
          }
        ]
      }
    ]
  }
```

- **List Data Schema** (`list_data.json`): A dict keyed by list ID, each containing `items` (with details and matched comments) and `unmatched_comments`:

```json
  {
    "F07SBP17R7Z": {
      "items": [
        {
          "record_id": "Rec07TF61CLDN",
          "list_id": "F07SBP17R7Z",
          "date_created": 1710000000,
          "created_by_id": "U12345ABC",
          "created_by_name": "john.doe",
          "title": "Fix SSL certificate issue",
          "fields": {
            "rich_text_notes": "Fix SSL certificate issue",
            "status": "in_progress",
            "owner": "john.doe",
            "date": "2024-03-10"
          },
          "comments": [
            {
              "ts": "1710000001.000200",
              "user_id": "U67890DEF",
              "user_name": "jane.smith",
              "text": "Looking into this issue now",
              "link": "https://workspace.slack.com/archives/C07SBP17R7Z/p1710000001000200?..."
            }
          ]
        }
      ],
      "unmatched_comments": [
        {
          "item_ts": "1710500000.000100",
          "item_text": "A comment was added",
          "comments": [...]
        }
      ]
    }
  }
```

- **Output**: `metadata.json` alongside `messages.json`, `list_comments.json`, and `list_data.json` records the channel ID, extraction timestamp, total message count, list statistics (including items matched with comments), and the user map.

---

## 5. Step 3 -- Data Sanitization (`data_sanitizer.py`)

### Data Flow

```mermaid
flowchart TD
    Start[Start] --> LoadRaw["Load messages.json from raw data dir"]
    LoadRaw --> LoadListData{"list_data.json exists?"}
    LoadListData -->|Yes| LoadLD["Load list_data.json"]
    LoadListData -->|No| LoadLegacy{"list_comments.json exists?"}
    LoadLegacy -->|Yes| LoadLC["Load list_comments.json\n(convert to list_data format)"]
    LoadLegacy -->|No| LoadExclusion
    LoadLD --> LoadExclusion[Load exclusion_rules.yaml]
    LoadLC --> LoadExclusion
    LoadExclusion --> BuildAnonMap["Build user_id -> @userN map\n(from messages + replies + list items + comments)"]
    BuildAnonMap --> BuildItemLookup["Build item_lookup: record_id -> item detail\n(from list_data items)"]
    BuildItemLookup --> IterateMsg[For each message + its replies]
    IterateMsg --> CheckExclude{"Excluded by author,\nmention, or link?"}
    CheckExclude -->|Yes| Skip[Skip]
    CheckExclude -->|No| Anonymize["Replace all <@UXXXX> with @userN"]
    Anonymize --> AppendOutput[Append to output text]
    AppendOutput --> IsListItem{"is_list_item\nand has list_refs?"}
    IsListItem -->|Yes| InlineEmit["Emit item fields + comments inline\nTrack record_id in emitted_rids"]
    IsListItem -->|No| ProcessReplies[Process thread replies]
    InlineEmit --> ProcessReplies
    ProcessReplies --> NextMsg{More messages?}
    Skip --> NextMsg
    NextMsg -->|Yes| IterateMsg
    NextMsg -->|No| HasLD{"list_data loaded?"}
    HasLD -->|Yes| IterateItems["For each list item NOT in emitted_rids:\n1. Output item title + fields\n2. Output matched comments"]
    HasLD -->|No| WriteFile
    IterateItems --> IterateUnmatched["For unmatched comments:\noutput in separate section"]
    IterateUnmatched --> WriteFile["Write all_messages.txt to slack_processed_data_<ts>/"]
    WriteFile --> SaveMap["Save anonymization_map.json"]
    SaveMap --> Done[Return output directory path]
```

### Key Implementation Details

- **Anonymization Map**: Scan all messages (including replies) AND list item comments to collect every unique `user_id`. Assign sequential dummy names: `@user1`, `@user2`, ... Store the mapping in `anonymization_map.json` for traceability (maps dummy name back to original user_id).
- **Exclusion Logic** (applied to every message, reply, AND list item comment independently):
  1. **Author exclusion**: If `message.user_id` is in `excluded_users` list, skip the entire message.
  2. **Mention exclusion**: Parse all `<@UXXXXXXX>` patterns in the message text. If ANY matched user_id is in `excluded_users`, skip the message.
  3. **Link exclusion**: If `message.link` is in `excluded_message_links`, skip the message.
- **User Mention Replacement**: After exclusion checks pass, use regex `r'<@(U[A-Z0-9]+)>'` to find all user mentions in the text and replace each with the corresponding `@userN` from the anonymization map.
- **Item Lookup Map**: Before iterating messages, the sanitizer builds a `record_id -> item detail` lookup dict from all items across all lists in `list_data`. This allows O(1) lookup when a channel message references a specific list item.
- **Inline List Item Emission**: When processing channel messages, if a message has `is_list_item=True` and its `list_refs` contain record IDs found in the item lookup, the item's field metadata and comments are emitted **inline** immediately after the message text. Each emitted record_id is tracked in an `emitted_rids` set to prevent duplication.
- **List Item Processing (dedicated section)**: The sanitizer loads `list_data.json` (preferred) or falls back to `list_comments.json` (legacy). After all channel messages are processed, remaining items (those **not** already emitted inline) are output in a dedicated "SLACK LIST ITEMS AND COMMENTS" section:
  1. Each list item is output with its title and field metadata (status, owner, date, etc.), followed immediately by its matched comments.
  2. Fields that duplicate the title are skipped. Field keys are title-cased with underscores replaced by spaces.
  3. Unmatched comments (where no item detail could be correlated) are output in a separate "Additional Comments" section per list.
  4. The same exclusion + anonymization rules apply to item fields and comments.
- **Output Format** (`all_messages.txt`): Channel messages first (ordered chronologically, thread replies indented). When a channel message references a list item, the item's fields and comments appear inline right after it. Then a dedicated section outputs remaining items not already emitted inline:

```
  [2024-03-10 14:30:00] @user1: Hello @user2 please review this
    [2024-03-10 14:31:00] @user2: Sure, looking at it now
    [2024-03-10 14:35:00] @user3: Done, LGTM
  [2024-03-10 14:45:00] @user1: <list URL for Fix SSL certificate issue>
    [Item: Fix SSL certificate issue]
    Status: in_progress | Owner: @user1 | Date: 2024-03-10
    [2024-03-10 14:46:00] @user2: Looking into this issue now
    [2024-03-10 14:50:00] @user3: Fixed in PR #42
  [2024-03-10 15:00:00] @user4: Deployment complete

  ============================================================
  SLACK LIST ITEMS AND COMMENTS
  ============================================================

  --- List F07SBP17R7Z ---

  [Item: Dashboard improvements]
    Status: completed | Owner: @user3

  --- Additional Comments (List F07SBP17R7Z) ---

  [List Item 2024-03-10 16:00:00]
    [2024-03-10 16:05:00] @user4: Some discussion here
```

  Note: "Fix SSL certificate issue" does NOT appear in the dedicated section because it was already emitted inline with the channel message that referenced it.

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
