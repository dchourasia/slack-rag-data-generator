"""
End-to-end demo: Fetch channel messages + thread replies (bot token),
detect Slack List items, fetch their details via slackLists.items.list,
fetch their comments from the hidden conversation (user token), and
correlate item details with comments.

Usage:
    python test_e2e_list_extraction.py \
        --bot-token xoxb-... \
        --user-token xoxp-... \
        --channel C07TF3MBMMW \
        --message-limit 50

Output: test_e2e_output.json  (raw data)
        test_e2e_preview.txt  (sanitized preview matching final output format)
"""

import argparse
import json
import re
import time
from datetime import datetime, timezone
from functools import partial

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

MAX_RETRIES = 5
DEFAULT_RETRY_DELAY = 1

LIST_URL_RE = re.compile(
    r"slack\.com/lists/[^/]+/(?P<list_id>F[A-Z0-9]+)\?record_id=(?P<record_id>Rec[A-Za-z0-9]+)"
)


def call_with_retry(api_method, **kwargs):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return api_method(**kwargs)
        except SlackApiError as exc:
            if exc.response.status_code == 429:
                wait = max(
                    int(exc.response.headers.get("Retry-After", DEFAULT_RETRY_DELAY)),
                    DEFAULT_RETRY_DELAY,
                )
                print(f"    Rate-limited (attempt {attempt}/{MAX_RETRIES}), waiting {wait}s …")
                time.sleep(wait)
            else:
                raise
    raise RuntimeError(f"API call failed after {MAX_RETRIES} retries")


def ts_to_str(ts: str) -> str:
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def fetch_messages(client: WebClient, channel: str, limit: int | None = None):
    messages, cursor = [], None
    while True:
        resp = call_with_retry(
            client.conversations_history, channel=channel, cursor=cursor, limit=200,
        )
        messages.extend(resp.get("messages", []))
        if limit and len(messages) >= limit:
            return messages[:limit]
        cursor = resp.get("response_metadata", {}).get("next_cursor")
        if not cursor:
            return messages


def fetch_replies(client: WebClient, channel: str, thread_ts: str):
    replies, cursor = [], None
    while True:
        resp = call_with_retry(
            client.conversations_replies,
            channel=channel, ts=thread_ts, cursor=cursor, limit=200,
        )
        for msg in resp.get("messages", []):
            if msg.get("ts") != thread_ts:
                replies.append(msg)
        cursor = resp.get("response_metadata", {}).get("next_cursor")
        if not cursor:
            return replies


def fetch_user_map(client: WebClient) -> dict[str, str]:
    user_map, cursor = {}, None
    while True:
        resp = call_with_retry(client.users_list, cursor=cursor, limit=200)
        for m in resp.get("members", []):
            profile = m.get("profile", {})
            name = (
                profile.get("display_name")
                or profile.get("real_name")
                or m.get("real_name")
                or m.get("name")
                or m["id"]
            )
            user_map[m["id"]] = name
        cursor = resp.get("response_metadata", {}).get("next_cursor")
        if not cursor:
            return user_map


def parse_list_refs(text: str) -> list[dict]:
    return [
        {"list_id": m.group("list_id"), "record_id": m.group("record_id")}
        for m in LIST_URL_RE.finditer(text)
    ]


# ---------------------------------------------------------------------------
# List item comments (hidden conversation, user token)
# ---------------------------------------------------------------------------

def build_list_comment_index(
    user_client: WebClient, list_ids: set[str],
) -> dict[str, list[dict]]:
    index: dict[str, list[dict]] = {}
    for list_id in sorted(list_ids):
        conv_id = "C" + list_id[1:]
        print(f"  List {list_id} → conversation {conv_id}")

        try:
            msgs = fetch_messages(user_client, conv_id)
        except SlackApiError as e:
            print(f"    ✗ Cannot access: {e.response['error']} (skipping)")
            continue

        print(f"    Fetched {len(msgs)} items")
        threaded = [m for m in msgs if m.get("reply_count", 0) > 0]
        print(f"    {len(threaded)} items have comments")

        items = []
        for i, msg in enumerate(threaded, 1):
            replies = fetch_replies(user_client, conv_id, msg["ts"])
            items.append({
                "item_ts": msg["ts"],
                "item_date": ts_to_str(msg["ts"]),
                "item_text": msg.get("text", ""),
                "comments": [
                    {
                        "ts": r["ts"],
                        "date": ts_to_str(r["ts"]),
                        "user_id": r.get("user", "UNKNOWN"),
                        "text": r.get("text", ""),
                    }
                    for r in replies
                ],
            })
            if i % 20 == 0:
                print(f"    Fetched comments for {i}/{len(threaded)} items …")

        total_comments = sum(len(it["comments"]) for it in items)
        print(f"    Total comments: {total_comments}")
        index[list_id] = items

    return index


# ---------------------------------------------------------------------------
# List item details (slackLists.items.list, bot or user token)
# ---------------------------------------------------------------------------

def parse_item_fields(fields: list[dict], user_map: dict[str, str]) -> dict:
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


def fetch_list_items(
    client: WebClient, list_id: str, user_map: dict[str, str],
    *, include_archived: bool = True,
) -> list[dict]:
    print(f"  Fetching item details for {list_id} via slackLists.items.list …")

    def _paginate(archived: bool = False) -> list[dict]:
        collected: list[dict] = []
        cursor: str | None = None
        while True:
            payload: dict = {"list_id": list_id, "limit": 100}
            if archived:
                payload["archived"] = True
            if cursor:
                payload["cursor"] = cursor
            api_fn = partial(client.api_call, "slackLists.items.list")
            resp = call_with_retry(api_fn, json=payload)
            collected.extend(resp.get("items", []))
            cursor = resp.get("response_metadata", {}).get("next_cursor")
            if not cursor:
                break
        return collected

    raw_items = _paginate(archived=False)
    active_count = len(raw_items)

    archived_count = 0
    if include_archived:
        try:
            archived_items = _paginate(archived=True)
            archived_count = len(archived_items)
            raw_items.extend(archived_items)
        except Exception:
            print(f"    Could not fetch archived items for {list_id} – skipping")

    print(f"    {len(raw_items)} items from Lists API ({active_count} active, {archived_count} archived)")

    parsed: list[dict] = []
    for item in raw_items:
        fields = parse_item_fields(item.get("fields", []), user_map)
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
            "created_by_name": user_map.get(item.get("created_by", ""), "UNKNOWN"),
            "title": title,
            "fields": fields,
        })
    return parsed


# ---------------------------------------------------------------------------
# Correlation: match items with comments via fuzzy timestamp (±5s window)
# ---------------------------------------------------------------------------

TS_MATCH_WINDOW = 5


def build_list_data(
    items: list[dict], comment_groups: list[dict],
) -> dict:
    cg_by_ts_int: list[tuple[int, dict]] = []
    for cg in comment_groups:
        cg_by_ts_int.append((int(float(cg["item_ts"])), cg))

    used_ts: set[str] = set()
    merged: list[dict] = []

    for item in items:
        entry = {**item, "comments": []}
        dc = item.get("date_created")
        if dc is not None:
            best_cg = None
            best_diff = TS_MATCH_WINDOW + 1
            for ts_int, cg in cg_by_ts_int:
                if cg["item_ts"] in used_ts:
                    continue
                diff = abs(ts_int - dc)
                if diff < best_diff:
                    best_diff = diff
                    best_cg = cg
            if best_cg is not None and best_diff <= TS_MATCH_WINDOW:
                entry["comments"] = best_cg["comments"]
                used_ts.add(best_cg["item_ts"])
        merged.append(entry)

    matched = sum(1 for e in merged if e["comments"])
    unmatched = [
        {
            "item_ts": cg["item_ts"],
            "item_text": cg.get("item_text", ""),
            "comments": cg["comments"],
        }
        for cg in comment_groups
        if cg["item_ts"] not in used_ts
    ]

    print(f"    Correlation: {matched}/{len(items)} items matched with comments, "
          f"{len(unmatched)} unmatched comment groups")
    return {"items": merged, "unmatched_comments": unmatched}


# ---------------------------------------------------------------------------
# Preview: generate sanitized output matching the final format
# ---------------------------------------------------------------------------

def generate_preview(
    records: list[dict],
    list_data: dict[str, dict],
    user_map: dict[str, str],
) -> str:
    item_lookup: dict[str, dict] = {}
    for ld in list_data.values():
        for item in ld.get("items", []):
            rid = item.get("record_id")
            if rid:
                item_lookup[rid] = item

    lines: list[str] = []

    for msg in records:
        uid = msg["user_id"]
        name = user_map.get(uid, uid)
        lines.append(f"[{msg['date']}] {name}: {msg['text'][:120]}")

        if msg.get("is_list_item") and item_lookup:
            seen: set[str] = set()
            for ref in msg.get("list_refs", []):
                rid = ref.get("record_id")
                if rid and rid not in seen and rid in item_lookup:
                    seen.add(rid)
                    item = item_lookup[rid]
                    title = item.get("title", "").strip()
                    lines.append(f"  [Item: {title or '(no title)'}]")
                    skip = {k for k, v in item.get("fields", {}).items() if v == title}
                    parts = [
                        f"{k.replace('_', ' ').title()}: {v}"
                        for k, v in item.get("fields", {}).items()
                        if k not in skip
                    ]
                    if parts:
                        lines.append("  " + " | ".join(parts))

        for reply in msg.get("thread_replies", []):
            r_name = user_map.get(reply["user_id"], reply["user_id"])
            lines.append(f"  [{reply['date']}] {r_name}: {reply['text'][:120]}")

    if list_data:
        lines.append("")
        lines.append("=" * 60)
        lines.append("SLACK LIST ITEMS AND COMMENTS")
        lines.append("=" * 60)

        for list_id in sorted(list_data.keys()):
            ld = list_data[list_id]
            items = ld.get("items", [])
            unmatched = ld.get("unmatched_comments", [])

            if not items and not unmatched:
                continue

            lines.append("")
            lines.append(f"--- List {list_id} ---")

            for item in items:
                title = item.get("title", "").strip()
                fields = item.get("fields", {})
                comments = item.get("comments", [])
                if not title and not fields and not comments:
                    continue

                lines.append("")
                lines.append(f"[Item: {title or '(no title)'}]")
                skip = {k for k, v in fields.items() if v == title}
                parts = [
                    f"{k.replace('_', ' ').title()}: {v}"
                    for k, v in fields.items()
                    if k not in skip
                ]
                if parts:
                    lines.append("  " + " | ".join(parts))
                for c in comments:
                    c_name = user_map.get(c["user_id"], c["user_id"])
                    c_ts = ts_to_str(c["ts"])
                    lines.append(f"  [{c_ts}] {c_name}: {c['text'][:120]}")

            if unmatched:
                lines.append("")
                lines.append(f"--- Additional Comments (List {list_id}) ---")
                for cg in unmatched:
                    if not cg.get("comments"):
                        continue
                    lines.append("")
                    lines.append(f"[List Item {ts_to_str(cg['item_ts'])}]")
                    for c in cg["comments"]:
                        c_name = user_map.get(c["user_id"], c["user_id"])
                        c_ts = ts_to_str(c["ts"])
                        lines.append(f"  [{c_ts}] {c_name}: {c['text'][:120]}")

    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser(
        description="E2E test: channel messages + list item details + comments",
    )
    p.add_argument("--bot-token", required=True, help="Bot token (xoxb-...)")
    p.add_argument("--user-token", required=True, help="User OAuth token (xoxp-...)")
    p.add_argument("--channel", required=True, help="Channel ID (e.g. C07TF3MBMMW)")
    p.add_argument("--message-limit", type=int, default=None, help="Max channel messages to fetch")
    args = p.parse_args()

    bot = WebClient(token=args.bot_token)
    user = WebClient(token=args.user_token)

    # --- Phase 1: Channel data (bot token) ---
    print("=" * 60)
    print("PHASE 1: Channel messages + thread replies (bot token)")
    print("=" * 60)

    print("Fetching user map …")
    user_map = fetch_user_map(bot)
    print(f"  {len(user_map)} users")

    print("Fetching channel messages …")
    raw_msgs = fetch_messages(bot, args.channel, limit=args.message_limit)
    print(f"  {len(raw_msgs)} messages")

    records = []
    all_list_ids: set[str] = set()

    for msg in raw_msgs:
        uid = msg.get("user", "UNKNOWN")
        ts = msg["ts"]
        text = msg.get("text", "")
        reply_count = msg.get("reply_count", 0)
        thread_ts = msg.get("thread_ts")

        list_refs = parse_list_refs(text)
        for ref in list_refs:
            all_list_ids.add(ref["list_id"])

        thread_replies = []
        if reply_count > 0 and thread_ts:
            for r in fetch_replies(bot, args.channel, thread_ts):
                r_text = r.get("text", "")
                r_refs = parse_list_refs(r_text)
                for ref in r_refs:
                    all_list_ids.add(ref["list_id"])
                thread_replies.append({
                    "ts": r["ts"],
                    "date": ts_to_str(r["ts"]),
                    "user_id": r.get("user", "UNKNOWN"),
                    "user_name": user_map.get(r.get("user", ""), r.get("user", "UNKNOWN")),
                    "text": r_text,
                    "list_refs": r_refs,
                })

        records.append({
            "ts": ts,
            "date": ts_to_str(ts),
            "user_id": uid,
            "user_name": user_map.get(uid, uid),
            "text": text,
            "reply_count": reply_count,
            "is_list_item": len(list_refs) > 0,
            "list_refs": list_refs,
            "thread_replies": thread_replies,
        })

    records.sort(key=lambda r: float(r["ts"]))
    list_item_msgs = sum(1 for r in records if r["is_list_item"])
    total_replies = sum(len(r["thread_replies"]) for r in records)
    print(f"  {list_item_msgs} messages reference list items")
    print(f"  {total_replies} total thread replies")
    print(f"  {len(all_list_ids)} unique list(s) found: {all_list_ids}")

    # --- Phase 2: List item comments (user token) ---
    print()
    print("=" * 60)
    print("PHASE 2: List item comments (user token)")
    print("=" * 60)

    list_comments: dict[str, list[dict]] = {}
    if all_list_ids:
        list_comments = build_list_comment_index(user, all_list_ids)
    else:
        print("  No list items found in channel messages.")

    # --- Phase 3: List item details (bot token, fallback user token) ---
    print()
    print("=" * 60)
    print("PHASE 3: List item details (slackLists.items.list)")
    print("=" * 60)

    list_data: dict[str, dict] = {}
    if all_list_ids:
        for list_id in sorted(all_list_ids):
            items_detail: list[dict] = []
            try:
                items_detail = fetch_list_items(bot, list_id, user_map)
            except SlackApiError as exc:
                err = exc.response.get("error", "")
                if err in ("missing_scope", "no_permission", "list_not_found"):
                    print(f"    Bot token failed ({err}), trying user token …")
                    try:
                        items_detail = fetch_list_items(user, list_id, user_map)
                    except SlackApiError as exc2:
                        print(f"    User token also failed: "
                              f"{exc2.response.get('error', str(exc2))} – skipping")
                else:
                    raise

            comment_groups = list_comments.get(list_id, [])
            list_data[list_id] = build_list_data(items_detail, comment_groups)
    else:
        print("  No lists to fetch.")

    # --- Phase 4: Write output ---
    print()
    print("=" * 60)
    print("PHASE 4: Writing output")
    print("=" * 60)

    total_items = sum(len(ld["items"]) for ld in list_data.values())
    total_matched = sum(
        sum(1 for it in ld["items"] if it["comments"])
        for ld in list_data.values()
    )
    total_list_comments = sum(
        sum(len(it["comments"]) for it in cg)
        for cg in list_comments.values()
    )

    output = {
        "channel_id": args.channel,
        "extraction_time": datetime.now(tz=timezone.utc).isoformat(),
        "summary": {
            "total_messages": len(records),
            "total_thread_replies": total_replies,
            "list_item_messages": list_item_msgs,
            "lists_found": list(all_list_ids),
            "total_list_items": total_items,
            "total_list_items_with_comments": sum(len(v) for v in list_comments.values()),
            "total_list_comments": total_list_comments,
            "total_items_matched_with_comments": total_matched,
        },
        "channel_messages": records,
        "list_data": list_data,
    }

    out_json = "test_e2e_output.json"
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"  Raw data → {out_json}")

    preview_text = generate_preview(records, list_data, user_map)
    out_preview = "test_e2e_preview.txt"
    with open(out_preview, "w", encoding="utf-8") as f:
        f.write(preview_text)
    print(f"  Preview  → {out_preview}")

    print()
    print("─── Summary ───")
    for k, v in output["summary"].items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
