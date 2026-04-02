"""
Step 1 – Extract the full message history (including thread replies) of a
Slack channel, plus Slack List item details and comments, and persist the
raw data as structured JSON.
"""

import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from functools import partial
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
    """Call a Slack API method with retry on rate-limit and network errors."""
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
        except (ConnectionError, TimeoutError, OSError) as exc:
            if attempt == MAX_RETRIES:
                raise
            wait = DEFAULT_RETRY_DELAY * (2 ** (attempt - 1))
            logger.warning(
                "Network error (attempt %d/%d): %s. Retrying in %ds …",
                attempt, MAX_RETRIES, exc, wait,
            )
            time.sleep(wait)
        except Exception as exc:
            if "IncompleteRead" in type(exc).__name__ or "IncompleteRead" in str(exc):
                if attempt == MAX_RETRIES:
                    raise
                wait = DEFAULT_RETRY_DELAY * (2 ** (attempt - 1))
                logger.warning(
                    "Incomplete response (attempt %d/%d): %s. Retrying in %ds …",
                    attempt, MAX_RETRIES, exc, wait,
                )
                time.sleep(wait)
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


RECORD_ID_RE = re.compile(r"Rec[A-Za-z0-9]+")


def _extract_record_id_from_message(msg: dict, list_id: str) -> str | None:
    """Try to extract a record_id from a hidden conversation message."""
    blocks = msg.get("blocks", [])
    for block in blocks:
        for element in block.get("elements", []):
            for child in element.get("elements", []):
                url = child.get("url", "")
                if list_id in url:
                    m = re.search(r"record_id=(Rec[A-Za-z0-9]+)", url)
                    if m:
                        return m.group(1)
                text_val = child.get("text", "")
                m = RECORD_ID_RE.search(text_val)
                if m:
                    return m.group(0)

    metadata = msg.get("metadata", {})
    event_payload = metadata.get("event_payload", {})
    rid = event_payload.get("record_id") or event_payload.get("item_id")
    if rid:
        return rid

    text = msg.get("text", "")
    m = re.search(r"record_id=(Rec[A-Za-z0-9]+)", text)
    if m:
        return m.group(1)

    return None


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

        record_id = _extract_record_id_from_message(item_msg, list_id)

        results.append({
            "list_id": list_id,
            "conversation_id": conv_id,
            "item_ts": item_ts,
            "item_text": item_msg.get("text", ""),
            "record_id": record_id,
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
# List item detail helpers (requires lists:read scope)
# ---------------------------------------------------------------------------

def _parse_item_fields(fields: list[dict], user_map: dict[str, str]) -> dict:
    """Extract key-value pairs from a list item's ``fields`` array."""
    parsed: dict[str, str] = {}
    for field in fields:
        key = field.get("key", field.get("column_id", "unknown"))
        if "text" in field and field["text"]:
            parsed[key] = field["text"]
        elif "select" in field:
            parsed[key] = ", ".join(field["select"])
        elif "date" in field:
            parsed[key] = ", ".join(field["date"])
        elif "user" in field:
            parsed[key] = ", ".join(
                user_map.get(uid, uid) for uid in field["user"]
            )
        elif "number" in field:
            parsed[key] = ", ".join(str(n) for n in field["number"])
        elif "checkbox" in field:
            cb = field["checkbox"]
            if isinstance(cb, list):
                parsed[key] = str(cb[0]) if cb else "False"
            else:
                parsed[key] = str(cb)
        elif "email" in field:
            parsed[key] = ", ".join(field["email"])
        elif "link" in field:
            parsed[key] = ", ".join(
                lnk.get("originalUrl", lnk.get("displayName", ""))
                for lnk in field["link"]
            )
        elif field.get("value") is not None:
            parsed[key] = str(field["value"])
    return parsed


def _fetch_list_items(
    client: WebClient,
    list_id: str,
    user_map: dict[str, str],
) -> list[dict]:
    """
    Fetch every item from a Slack List via ``slackLists.items.list``.

    Returns a list of dicts with ``record_id``, ``title``, ``fields``, etc.
    Requires the ``lists:read`` OAuth scope on the token.
    """
    logger.info("Fetching list item details for %s via slackLists.items.list …", list_id)
    raw_items: list[dict] = []
    cursor: str | None = None
    while True:
        payload: dict = {"list_id": list_id, "limit": 100}
        if cursor:
            payload["cursor"] = cursor
        api_fn = partial(client.api_call, "slackLists.items.list")
        resp = _call_with_retry(api_fn, json=payload)
        raw_items.extend(resp.get("items", []))
        cursor = resp.get("response_metadata", {}).get("next_cursor")
        if not cursor:
            break

    logger.info("  List %s: %d items from Lists API", list_id, len(raw_items))

    parsed: list[dict] = []
    for item in raw_items:
        fields = _parse_item_fields(item.get("fields", []), user_map)
        title = ""
        for f in item.get("fields", []):
            if f.get("text"):
                title = f["text"]
                break
        parsed.append({
            "record_id": item["id"],
            "list_id": item.get("list_id", list_id),
            "date_created": item.get("date_created"),
            "created_by_id": item.get("created_by", "UNKNOWN"),
            "created_by_name": user_map.get(
                item.get("created_by", ""), "UNKNOWN"
            ),
            "title": title,
            "fields": fields,
        })
    return parsed


def _build_list_data(
    items: list[dict],
    comment_groups: list[dict],
) -> dict:
    """
    Merge list item details with comment groups from the hidden conversation.

    Correlation strategy (in priority order):
    1. Match by ``record_id`` extracted from the hidden conversation message
       blocks/metadata against item ``record_id`` from the Lists API.
    2. Fall back to ``date_created`` (integer) matched against
       ``int(float(item_ts))`` for 1-second-precision timestamp matching.
    Unmatched comment groups are preserved separately.
    """
    rid_map: dict[str, dict] = {}
    ts_map: dict[int, list[dict]] = {}
    for cg in comment_groups:
        rid = cg.get("record_id")
        if rid:
            rid_map[rid] = cg
        ts_int = int(float(cg["item_ts"]))
        ts_map.setdefault(ts_int, []).append(cg)

    used_ts: set[str] = set()
    merged: list[dict] = []
    matched_by_rid = 0
    matched_by_ts = 0

    for item in items:
        entry = {**item, "comments": []}
        rid = item.get("record_id")

        if rid and rid in rid_map:
            cg = rid_map[rid]
            if cg["item_ts"] not in used_ts:
                entry["comments"] = cg["comments"]
                used_ts.add(cg["item_ts"])
                matched_by_rid += 1
        else:
            dc = item.get("date_created")
            if dc is not None:
                for cg in ts_map.get(dc, []):
                    if cg["item_ts"] not in used_ts:
                        entry["comments"] = cg["comments"]
                        used_ts.add(cg["item_ts"])
                        matched_by_ts += 1
                        break
        merged.append(entry)

    total_matched = matched_by_rid + matched_by_ts
    unmatched_comments = [
        {
            "item_ts": cg["item_ts"],
            "item_text": cg.get("item_text", ""),
            "comments": cg["comments"],
        }
        for cg in comment_groups
        if cg["item_ts"] not in used_ts
    ]

    logger.info(
        "  Correlation: %d/%d items matched with comments "
        "(%d by record_id, %d by timestamp), %d unmatched comment groups",
        total_matched, len(items), matched_by_rid, matched_by_ts,
        len(unmatched_comments),
    )
    return {"items": merged, "unmatched_comments": unmatched_comments}


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

    # 6. Fetch list item details (requires lists:read scope)
    list_data: dict[str, dict] = {}
    if all_list_ids:
        for list_id in sorted(all_list_ids):
            items_detail: list[dict] = []
            try:
                items_detail = _fetch_list_items(client, list_id, user_map)
            except SlackApiError as exc:
                err = exc.response.get("error", "")
                if err in ("missing_scope", "no_permission", "list_not_found"):
                    logger.warning(
                        "Bot token cannot fetch items for list %s (%s). "
                        "Trying user token …",
                        list_id, err,
                    )
                    if user_client:
                        try:
                            items_detail = _fetch_list_items(
                                user_client, list_id, user_map,
                            )
                        except SlackApiError as exc2:
                            logger.warning(
                                "User token also failed for list %s: %s – "
                                "item details will be unavailable",
                                list_id,
                                exc2.response.get("error", str(exc2)),
                            )
                    else:
                        logger.warning(
                            "No user token set – item details unavailable "
                            "for list %s. Add lists:read scope to enable.",
                            list_id,
                        )
                else:
                    raise

            comment_groups = list_comments.get(list_id, [])
            list_data[list_id] = _build_list_data(items_detail, comment_groups)
    elif list_comments:
        for list_id, comment_groups in list_comments.items():
            list_data[list_id] = _build_list_data([], comment_groups)

    # 7. Persist
    messages_path = out_dir / "messages.json"
    with open(messages_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    if list_comments:
        with open(out_dir / "list_comments.json", "w", encoding="utf-8") as f:
            json.dump(list_comments, f, ensure_ascii=False, indent=2)

    if list_data:
        with open(out_dir / "list_data.json", "w", encoding="utf-8") as f:
            json.dump(list_data, f, ensure_ascii=False, indent=2)

    total_list_comments = sum(
        sum(len(it["comments"]) for it in items)
        for items in list_comments.values()
    )
    total_list_items = sum(
        len(ld["items"]) for ld in list_data.values()
    )
    total_matched = sum(
        sum(1 for it in ld["items"] if it["comments"])
        for ld in list_data.values()
    )
    metadata = {
        "channel_id": channel_id,
        "workspace_url": workspace_url,
        "extraction_timestamp": timestamp,
        "total_messages": len(records),
        "total_replies": sum(len(r["replies"]) for r in records),
        "list_item_messages": sum(1 for r in records if r["is_list_item"]),
        "lists_found": sorted(all_list_ids),
        "total_list_items": total_list_items,
        "total_list_items_with_comments": sum(len(v) for v in list_comments.values()),
        "total_list_comments": total_list_comments,
        "total_items_matched_with_comments": total_matched,
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
    if list_data:
        logger.info(
            "List item details – %d items fetched, %d matched with comments",
            total_list_items, total_matched,
        )
    return str(out_dir.resolve())
