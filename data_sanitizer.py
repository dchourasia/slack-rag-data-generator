"""
Step 2 – Sanitize raw Slack data: apply exclusion rules, anonymize user
mentions, and produce a single plain-text file of cleaned messages.
"""

import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

USER_MENTION_RE = re.compile(r"<@(U[A-Z0-9]+)>")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_exclusion_rules(path: str = "exclusion_rules.yaml") -> dict:
    with open(path, "r") as f:
        rules = yaml.safe_load(f) or {}
    return {
        "excluded_users": set(rules.get("excluded_users", [])),
        "excluded_message_links": set(rules.get("excluded_message_links", [])),
    }


def _ts_to_datetime(ts: str) -> str:
    """Convert a Slack timestamp to a human-readable UTC string."""
    epoch = float(ts)
    dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _build_anonymization_map(messages: list[dict]) -> dict[str, str]:
    """
    Scan every message and reply to collect unique user IDs, then assign
    sequential anonymous names (@user1, @user2, …).
    """
    unique_ids: list[str] = []
    seen: set[str] = set()

    def _register(uid: str) -> None:
        if uid and uid not in seen:
            seen.add(uid)
            unique_ids.append(uid)

    for msg in messages:
        _register(msg.get("user_id"))
        for mention_id in USER_MENTION_RE.findall(msg.get("text", "")):
            _register(mention_id)
        for reply in msg.get("replies", []):
            _register(reply.get("user_id"))
            for mention_id in USER_MENTION_RE.findall(reply.get("text", "")):
                _register(mention_id)

    return {uid: f"@user{i}" for i, uid in enumerate(unique_ids, start=1)}


def _should_exclude(
    msg_record: dict,
    excluded_users: set[str],
    excluded_links: set[str],
) -> bool:
    """Return True if this message/reply should be dropped."""
    if msg_record.get("user_id") in excluded_users:
        return True
    if msg_record.get("link") in excluded_links:
        return True
    mentioned_ids = set(USER_MENTION_RE.findall(msg_record.get("text", "")))
    if mentioned_ids & excluded_users:
        return True
    return False


def _anonymize_text(text: str, anon_map: dict[str, str]) -> str:
    """Replace every ``<@UXXXX>`` token with its anonymous alias."""
    def _replacer(match: re.Match) -> str:
        uid = match.group(1)
        return anon_map.get(uid, f"@unknown_{uid}")
    return USER_MENTION_RE.sub(_replacer, text)


# ---------------------------------------------------------------------------
# Main sanitization
# ---------------------------------------------------------------------------

def sanitize_data(raw_data_dir: str, config: dict | None = None) -> str:
    """
    Read raw extraction output, apply exclusions + anonymization, and write
    the cleaned result as a single text file.

    Parameters
    ----------
    raw_data_dir : str
        Path to the ``slack_raw_data_<ts>`` directory produced by Step 1.
    config : dict, optional
        Parsed config.yaml contents.

    Returns
    -------
    str
        Absolute path to the ``slack_processed_data_<ts>`` directory.
    """
    if config is None:
        import yaml as _y
        with open("config.yaml", "r") as f:
            config = _y.safe_load(f)

    raw_path = Path(raw_data_dir)
    messages_file = raw_path / "messages.json"
    if not messages_file.exists():
        raise FileNotFoundError(f"messages.json not found in {raw_path}")

    with open(messages_file, "r", encoding="utf-8") as f:
        messages: list[dict] = json.load(f)

    rules = _load_exclusion_rules()
    excluded_users = rules["excluded_users"]
    excluded_links = rules["excluded_message_links"]

    anon_map = _build_anonymization_map(messages)

    base_dir = Path(config["output"]["base_dir"])
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = base_dir / f"slack_processed_data_{timestamp}"
    out_dir.mkdir(parents=True, exist_ok=True)

    lines: list[str] = []
    kept_messages = 0
    skipped_messages = 0
    kept_replies = 0
    skipped_replies = 0

    for msg in messages:
        if _should_exclude(msg, excluded_users, excluded_links):
            skipped_messages += 1
            continue

        kept_messages += 1
        author = anon_map.get(msg["user_id"], f"@unknown_{msg['user_id']}")
        text = _anonymize_text(msg.get("text", ""), anon_map)
        ts_str = _ts_to_datetime(msg["ts"])
        lines.append(f"[{ts_str}] {author}: {text}")

        for reply in msg.get("replies", []):
            if _should_exclude(reply, excluded_users, excluded_links):
                skipped_replies += 1
                continue
            kept_replies += 1
            r_author = anon_map.get(
                reply["user_id"], f"@unknown_{reply['user_id']}"
            )
            r_text = _anonymize_text(reply.get("text", ""), anon_map)
            r_ts = _ts_to_datetime(reply["ts"])
            lines.append(f"  [{r_ts}] {r_author}: {r_text}")

    output_text = "\n".join(lines) + "\n" if lines else ""
    out_file = out_dir / "all_messages.txt"
    with open(out_file, "w", encoding="utf-8") as f:
        f.write(output_text)

    anon_map_inverted = {v: k for k, v in anon_map.items()}
    with open(out_dir / "anonymization_map.json", "w", encoding="utf-8") as f:
        json.dump(
            {"anon_to_real": anon_map_inverted, "real_to_anon": anon_map},
            f,
            ensure_ascii=False,
            indent=2,
        )

    logger.info(
        "Sanitization complete – kept %d messages (%d replies), "
        "skipped %d messages (%d replies). Output: %s",
        kept_messages, kept_replies,
        skipped_messages, skipped_replies,
        out_file,
    )
    return str(out_dir.resolve())
