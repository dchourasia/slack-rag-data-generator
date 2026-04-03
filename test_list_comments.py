"""
Demo: Fetch Slack List item comments using the Hybrid approach.

Usage:
    python test_list_comments.py --user-token xoxp-... --list-id F07SBP17R7Z [--limit 5]

This script:
  1. Derives the hidden list conversation ID (F... → C...)
  2. Fetches messages from the list conversation (each message = a list item)
  3. Fetches thread replies (= item comments) for each threaded message
  4. Writes the results to list_comments_<list_id>.json
"""

import argparse
import json
import time
from datetime import datetime, timezone

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

MAX_RETRIES = 5
DEFAULT_RETRY_DELAY = 1


def call_with_retry(api_method, **kwargs):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return api_method(**kwargs)
        except SlackApiError as exc:
            if exc.response.status_code == 429:
                wait = max(int(exc.response.headers.get("Retry-After", DEFAULT_RETRY_DELAY)), DEFAULT_RETRY_DELAY)
                print(f"  Rate-limited (attempt {attempt}/{MAX_RETRIES}), waiting {wait}s …")
                time.sleep(wait)
            else:
                raise
    raise RuntimeError(f"API call failed after {MAX_RETRIES} retries")


def ts_to_str(ts: str) -> str:
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def fetch_list_conversation(client: WebClient, conv_id: str, limit: int | None = None):
    """Fetch messages from the list conversation (each = a list item thread parent)."""
    messages = []
    cursor = None
    while True:
        resp = call_with_retry(
            client.conversations_history,
            channel=conv_id,
            cursor=cursor,
            limit=200,
        )
        messages.extend(resp.get("messages", []))
        if limit and len(messages) >= limit:
            messages = messages[:limit]
            break
        cursor = resp.get("response_metadata", {}).get("next_cursor")
        if not cursor:
            break
    return messages


def fetch_replies(client: WebClient, conv_id: str, thread_ts: str):
    """Fetch all replies for a thread, excluding the parent."""
    replies = []
    cursor = None
    while True:
        resp = call_with_retry(
            client.conversations_replies,
            channel=conv_id,
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


def main():
    parser = argparse.ArgumentParser(description="Test fetching Slack List item comments")
    parser.add_argument("--user-token", required=True, help="User OAuth token (xoxp-...)")
    parser.add_argument("--list-id", required=True, help="Slack List file ID (e.g. F07SBP17R7Z)")
    parser.add_argument("--limit", type=int, default=None, help="Max list items to fetch (default: all)")
    args = parser.parse_args()

    conv_id = "C" + args.list_id[1:]
    print(f"List ID:         {args.list_id}")
    print(f"Conversation ID: {conv_id} (derived)")
    print()

    client = WebClient(token=args.user_token)

    # Step 1: Verify access
    print("Step 1: Testing access to list conversation …")
    try:
        test = client.conversations_history(channel=conv_id, limit=1)
        print(f"  ✓ Access confirmed ({test['ok']})")
    except SlackApiError as e:
        print(f"  ✗ Access failed: {e.response['error']}")
        print()
        if e.response["error"] == "channel_not_found":
            print("The user token cannot see this list conversation.")
            print("Possible fixes:")
            print("  - Ensure the token owner has access to the list")
            print("  - Check that channels:history scope is granted as a User Token Scope")
        return

    # Step 2: Fetch list item messages
    print("Step 2: Fetching list item messages …")
    messages = fetch_list_conversation(client, conv_id, limit=args.limit)
    print(f"  Fetched {len(messages)} messages")

    threaded = [m for m in messages if m.get("reply_count", 0) > 0]
    print(f"  Of which {len(threaded)} have comments (reply_count > 0)")
    print()

    # Step 3: Fetch comments for each threaded item
    print("Step 3: Fetching comments for threaded items …")
    results = []
    for i, msg in enumerate(threaded, 1):
        thread_ts = msg["ts"]
        replies = fetch_replies(client, conv_id, thread_ts)
        item_data = {
            "item_ts": thread_ts,
            "item_text": msg.get("text", ""),
            "item_date": ts_to_str(thread_ts),
            "reply_count": msg.get("reply_count", 0),
            "comments": [
                {
                    "ts": r["ts"],
                    "date": ts_to_str(r["ts"]),
                    "user": r.get("user", "UNKNOWN"),
                    "text": r.get("text", ""),
                }
                for r in replies
            ],
        }
        results.append(item_data)
        if i % 10 == 0 or i == len(threaded):
            print(f"  Processed {i}/{len(threaded)} items")

    # Step 4: Write output
    out_file = f"list_comments_{args.list_id}.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    total_comments = sum(len(r["comments"]) for r in results)
    print()
    print(f"Done! {len(results)} items with {total_comments} total comments → {out_file}")

    # Preview
    if results:
        print()
        print("─── Preview (first item with comments) ───")
        first = results[0]
        print(f"  Item: {first['item_text'][:120]}")
        print(f"  Date: {first['item_date']}")
        print(f"  Comments ({len(first['comments'])}):")
        for c in first["comments"][:3]:
            print(f"    [{c['date']}] {c['user']}: {c['text'][:100]}")
        if len(first["comments"]) > 3:
            print(f"    … and {len(first['comments']) - 3} more")


if __name__ == "__main__":
    main()
