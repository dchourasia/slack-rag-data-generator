"""
Step 1 – Extract the full message history (including thread replies) of a
Slack channel, plus comments on Slack List items, and persist the raw data
as structured JSON.
"""

import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import yaml
from dotenv import load_dotenv
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

logger = logging.getLogger(__name__)

MAX_RETRIES = 5
DEFAULT_RETRY_DELAY = 1  # seconds

LIST_URL_RE = re.compile(
    r"slack\.com/lists/[^/]+/(?P<list_id>F[A-Z0-9]+)\?record_id=(?P<record_id>Rec[A-Za-z0-9]+)"
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_config(config_path: str = "config.yaml") -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def _slack_client() -> WebClient:
    load_dotenv()
    token = os.getenv("SLACK_BOT_TOKEN")
    if not token:
        raise RuntimeError("SLACK_BOT_TOKEN is not set in .env")
    return WebClient(token=token)


def _slack_user_client() -> WebClient | None:
    """Return a user-token WebClient for list comment access, or None."""
    load_dotenv()
    token = os.getenv("SLACK_USER_TOKEN")
    if not token:
        return None
    return WebClient(token=token)


def _parse_list_refs(text: str) -> list[dict]:
    """Extract all list_id + record_id pairs from a message's text."""
    return [
        {"list_id": m.group("list_id"), "record_id": m.group("record_id")}
        for m in LIST_URL_RE.finditer(text)
    ]


def _call_with_retry(api_method, **kwargs):
    """Call a Slack API method with automatic retry on rate-limit (429)."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return api_method(**kwargs)
        except SlackApiError as exc:
            if exc.response.status_code == 429:
                retry_after = int(
                    exc.response.headers.get("Retry-After", DEFAULT_RETRY_DELAY)
                )
                retry_after = max(retry_after, DEFAULT_RETRY_DELAY)
                logger.warning(
                    "Rate-limited (attempt %d/%d). Sleeping %ds …",
                    attempt, MAX_RETRIES, retry_after,
                )
                time.sleep(retry_after)
            else:
                raise
    raise RuntimeError(f"Slack API call failed after {MAX_RETRIES} retries")


def _build_message_link(
    workspace_url: str, channel_id: str, ts: str, thread_ts: str | None = None
) -> str:
    ts_compact = ts.replace(".", "")
    base = f"{workspace_url}/archives/{channel_id}/p{ts_compact}"
    if thread_ts and thread_ts != ts:
        thread_ts_compact = thread_ts.replace(".", "")
        base += f"?thread_ts={thread_ts_compact}&cid={channel_id}"
    return base


# ---------------------------------------------------------------------------
# Fetch helpers
# ---------------------------------------------------------------------------

def _fetch_all_users(client: WebClient) -> dict[str, str]:
    """Return a mapping of user_id -> display_name for every workspace user."""
    user_map: dict[str, str] = {}
    cursor = None
    while True:
        resp = _call_with_retry(
            client.users_list, cursor=cursor, limit=200
        )
        for member in resp.get("members", []):
            uid = member["id"]
            profile = member.get("profile", {})
            name = (
                profile.get("display_name")
                or profile.get("real_name")
                or member.get("real_name")
                or member.get("name")
                or uid
            )
            user_map[uid] = name
        cursor = resp.get("response_metadata", {}).get("next_cursor")
        if not cursor:
            break
    logger.info("Fetched %d users from workspace", len(user_map))
    return user_map


def _fetch_channel_messages(
    client: WebClient, channel_id: str
) -> list[dict]:
    """Fetch every message in *channel_id* using cursor-based pagination."""
    messages: list[dict] = []
    cursor = None
    while True:
        resp = _call_with_retry(
            client.conversations_history,
            channel=channel_id,
            cursor=cursor,
            limit=200,
        )
        messages.extend(resp.get("messages", []))
        cursor = resp.get("response_metadata", {}).get("next_cursor")
        if not cursor:
            break
        logger.info("Fetched %d messages so far …", len(messages))
    logger.info("Total top-level messages fetched: %d", len(messages))
    return messages


def _fetch_thread_replies(
    client: WebClient, channel_id: str, thread_ts: str
) -> list[dict]:
    """Fetch all replies for a given thread (excluding the parent message)."""
    replies: list[dict] = []
    cursor = None
    while True:
        resp = _call_with_retry(
            client.conversations_replies,
            channel=channel_id,
            ts=thread_ts,
            cursor=cursor,
            limit=200,
        )
        for msg in resp.get("messages", []):
            if msg.get("ts") != thread_ts:
                replies.append(msg)
        cursor = resp.get("response_metadata", {}).get("next_cursor")
        if not cursor:
            break
    return replies


# ---------------------------------------------------------------------------
# List item comment helpers
# ---------------------------------------------------------------------------

def _list_id_to_conversation(list_id: str) -> str:
    """Derive the hidden companion conversation ID from a list file ID."""
    return "C" + list_id[1:]


def _fetch_list_item_comments(
    user_client: WebClient,
    list_id: str,
    user_map: dict[str, str],
    workspace_url: str,
) -> list[dict]:
    """
    Fetch all items with comments from a Slack List's hidden conversation.

    Each list item is stored as a message in a companion conversation whose
    ID mirrors the list file ID with a ``C`` prefix instead of ``F``.
    Comments on the item are thread replies in that conversation.
    Requires a **user token** -- bot tokens cannot access these conversations.
    """
    conv_id = _list_id_to_conversation(list_id)
    logger.info(
        "Fetching list item comments for %s (conversation %s) …",
        list_id, conv_id,
    )

    # Fetch all messages in the list conversation (each = a list item)
    items: list[dict] = []
    cursor = None
    while True:
        resp = _call_with_retry(
            user_client.conversations_history,
            channel=conv_id,
            cursor=cursor,
            limit=200,
        )
        items.extend(resp.get("messages", []))
        cursor = resp.get("response_metadata", {}).get("next_cursor")
        if not cursor:
            break

    threaded = [m for m in items if m.get("reply_count", 0) > 0]
    logger.info(
        "  List %s: %d items, %d with comments",
        list_id, len(items), len(threaded),
    )

    results: list[dict] = []
    for item_msg in threaded:
        item_ts = item_msg["ts"]
        replies = _fetch_thread_replies(user_client, conv_id, item_ts)
        results.append({
            "list_id": list_id,
            "conversation_id": conv_id,
            "item_ts": item_ts,
            "item_text": item_msg.get("text", ""),
            "comments": [
                {
                    "ts": r["ts"],
                    "user_id": r.get("user", "UNKNOWN"),
                    "user_name": user_map.get(r.get("user", ""), r.get("user", "UNKNOWN")),
                    "text": r.get("text", ""),
                    "link": _build_message_link(
                        workspace_url, conv_id, r["ts"], item_ts
                    ),
                }
                for r in replies
            ],
        })

    total_comments = sum(len(it["comments"]) for it in results)
    logger.info(
        "  List %s: fetched %d comments across %d items",
        list_id, total_comments, len(results),
    )
    return results


# ---------------------------------------------------------------------------
# Main extraction
# ---------------------------------------------------------------------------

def extract_slack_data(config: dict | None = None) -> str:
    """
    Extract the full channel history and return the output directory path.

    Parameters
    ----------
    config : dict, optional
        Parsed config.yaml contents.  Loaded from disk when *None*.

    Returns
    -------
    str
        Absolute path to the ``slack_raw_data_<timestamp>`` directory.
    """
    if config is None:
        config = _load_config()

    channel_id: str = config["slack"]["channel_id"]
    workspace_url: str = config["slack"]["workspace_url"].rstrip("/")
    base_dir = Path(config["output"]["base_dir"])

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = base_dir / f"slack_raw_data_{timestamp}"
    out_dir.mkdir(parents=True, exist_ok=True)

    client = _slack_client()
    user_client = _slack_user_client()

    # 1. User map
    user_map = _fetch_all_users(client)

    # 2. Channel messages
    raw_messages = _fetch_channel_messages(client, channel_id)

    # 3. Build structured records (with thread replies)
    records: list[dict] = []
    all_list_ids: set[str] = set()
    total = len(raw_messages)

    for idx, msg in enumerate(raw_messages, 1):
        user_id = msg.get("user", "UNKNOWN")
        ts = msg["ts"]
        text = msg.get("text", "")
        thread_ts = msg.get("thread_ts")
        reply_count = msg.get("reply_count", 0)

        list_refs = _parse_list_refs(text)
        for ref in list_refs:
            all_list_ids.add(ref["list_id"])

        record: dict = {
            "ts": ts,
            "user_id": user_id,
            "user_name": user_map.get(user_id, user_id),
            "text": text,
            "link": _build_message_link(workspace_url, channel_id, ts),
            "thread_ts": thread_ts,
            "reply_count": reply_count,
            "replies": [],
            "is_list_item": len(list_refs) > 0,
            "list_refs": list_refs,
        }

        if reply_count > 0 and thread_ts:
            raw_replies = _fetch_thread_replies(client, channel_id, thread_ts)
            for reply in raw_replies:
                r_user = reply.get("user", "UNKNOWN")
                r_ts = reply["ts"]
                r_text = reply.get("text", "")
                for ref in _parse_list_refs(r_text):
                    all_list_ids.add(ref["list_id"])
                record["replies"].append(
                    {
                        "ts": r_ts,
                        "user_id": r_user,
                        "user_name": user_map.get(r_user, r_user),
                        "text": r_text,
                        "link": _build_message_link(
                            workspace_url, channel_id, r_ts, thread_ts
                        ),
                    }
                )

        records.append(record)
        if idx % 100 == 0 or idx == total:
            logger.info("Processed %d/%d messages", idx, total)

    # 4. Sort chronologically (oldest first)
    records.sort(key=lambda r: float(r["ts"]))

    # 5. Fetch list item comments (requires user token)
    list_comments: dict[str, list[dict]] = {}
    if all_list_ids and user_client:
        logger.info(
            "Found %d unique list(s) referenced in messages: %s",
            len(all_list_ids), ", ".join(sorted(all_list_ids)),
        )
        for list_id in sorted(all_list_ids):
            try:
                list_comments[list_id] = _fetch_list_item_comments(
                    user_client, list_id, user_map, workspace_url,
                )
            except SlackApiError as exc:
                logger.warning(
                    "Could not access list %s (conversation %s): %s – skipping",
                    list_id, _list_id_to_conversation(list_id),
                    exc.response.get("error", str(exc)),
                )
    elif all_list_ids and not user_client:
        logger.warning(
            "Found %d list(s) but SLACK_USER_TOKEN is not set – "
            "list item comments will NOT be extracted. "
            "Set SLACK_USER_TOKEN in .env to enable this feature.",
            len(all_list_ids),
        )

    # 6. Persist
    messages_path = out_dir / "messages.json"
    with open(messages_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    if list_comments:
        with open(out_dir / "list_comments.json", "w", encoding="utf-8") as f:
            json.dump(list_comments, f, ensure_ascii=False, indent=2)

    total_list_comments = sum(
        sum(len(it["comments"]) for it in items)
        for items in list_comments.values()
    )
    metadata = {
        "channel_id": channel_id,
        "workspace_url": workspace_url,
        "extraction_timestamp": timestamp,
        "total_messages": len(records),
        "total_replies": sum(len(r["replies"]) for r in records),
        "list_item_messages": sum(1 for r in records if r["is_list_item"]),
        "lists_found": sorted(all_list_ids),
        "total_list_items_with_comments": sum(len(v) for v in list_comments.values()),
        "total_list_comments": total_list_comments,
        "user_map": user_map,
    }
    with open(out_dir / "metadata.json", "w", encoding="utf-8") as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)

    logger.info(
        "Extraction complete – %d messages (%d with threads, %d list items) "
        "saved to %s",
        len(records),
        sum(1 for r in records if r["replies"]),
        sum(1 for r in records if r["is_list_item"]),
        out_dir,
    )
    if list_comments:
        logger.info(
            "List item comments – %d items with %d total comments across %d list(s)",
            sum(len(v) for v in list_comments.values()),
            total_list_comments,
            len(list_comments),
        )
    return str(out_dir.resolve())
