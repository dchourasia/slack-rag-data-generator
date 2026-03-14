"""
Step 0 – Fetch opt-out responses from a Google Form and merge them into
exclusion_rules.yaml before the rest of the pipeline runs.
"""

import logging
import os
import time
from pathlib import Path

import yaml
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)

RESPONSES_SCOPE = "https://www.googleapis.com/auth/forms.responses.readonly"
FORM_BODY_SCOPE = "https://www.googleapis.com/auth/forms.body.readonly"

MAX_RETRIES = 5
EXCLUSION_RULES_PATH = "exclusion_rules.yaml"


class GoogleFormExtractor:
    """Fetch Google Form opt-out responses and update exclusion rules."""

    def __init__(self, config: dict) -> None:
        load_dotenv()

        form_cfg = config.get("google_form", {})
        self.form_id: str = form_cfg["form_id"]
        self.user_id_qid: str = form_cfg["user_id_question_id"]
        self.links_qid: str = form_cfg["message_links_question_id"]

        key_file = os.getenv("GOOGLE_SERVICE_ACCOUNT_KEY_FILE")
        if not key_file:
            raise RuntimeError(
                "GOOGLE_SERVICE_ACCOUNT_KEY_FILE is not set in .env"
            )
        if not Path(key_file).is_file():
            raise FileNotFoundError(
                f"Service account key file not found: {key_file}"
            )

        self._key_file = key_file

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_service(self, scopes: list[str]):
        creds = service_account.Credentials.from_service_account_file(
            self._key_file, scopes=scopes
        )
        return build("forms", "v1", credentials=creds)

    @staticmethod
    def _call_with_retry(callable_fn):
        """Execute *callable_fn* with retry on rate-limit / transient errors."""
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                return callable_fn()
            except HttpError as exc:
                status = exc.resp.status
                if status in (429, 500, 503):
                    retry_after = int(
                        exc.resp.get("retry-after", attempt)
                    )
                    retry_after = max(retry_after, 1)
                    logger.warning(
                        "Google API %d (attempt %d/%d). Retrying in %ds …",
                        status, attempt, MAX_RETRIES, retry_after,
                    )
                    time.sleep(retry_after)
                else:
                    raise
        raise RuntimeError(
            f"Google API call failed after {MAX_RETRIES} retries"
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def fetch_and_update_exclusion_rules(self) -> None:
        """Fetch all form responses, parse them, and merge into the YAML."""
        service = self._build_service([RESPONSES_SCOPE])

        all_responses: list[dict] = []
        request = service.forms().responses().list(formId=self.form_id)

        while request is not None:
            result = self._call_with_retry(request.execute)
            all_responses.extend(result.get("responses", []))
            request = service.forms().responses().list_next(request, result)

        logger.info(
            "Fetched %d responses from Google Form %s",
            len(all_responses), self.form_id,
        )

        new_users: set[str] = set()
        new_links: set[str] = set()

        for resp in all_responses:
            answers = resp.get("answers", {})

            q1 = answers.get(self.user_id_qid, {})
            text_answers = q1.get("textAnswers", {}).get("answers", [])
            if text_answers:
                uid = text_answers[0].get("value", "").strip()
                if uid:
                    new_users.add(uid)

            q2 = answers.get(self.links_qid, {})
            text_answers = q2.get("textAnswers", {}).get("answers", [])
            if text_answers:
                raw_links = text_answers[0].get("value", "").strip()
                for line in raw_links.splitlines():
                    link = line.strip()
                    if link:
                        new_links.add(link)

        logger.info(
            "Parsed form data: %d user IDs, %d message links",
            len(new_users), len(new_links),
        )

        self._merge_exclusion_rules(new_users, new_links)

    def print_form_structure(self) -> None:
        """Print every question's title and ID (setup helper)."""
        service = self._build_service([FORM_BODY_SCOPE])
        result = self._call_with_retry(
            lambda: service.forms().get(formId=self.form_id).execute()
        )

        print(f"\nForm: {result.get('info', {}).get('title', '(untitled)')}")
        print(f"Form ID: {self.form_id}\n")

        for item in result.get("items", []):
            title = item.get("title", "(no title)")
            question = item.get("questionItem", {}).get("question", {})
            qid = question.get("questionId", "N/A")
            print(f"  Question: {title}")
            print(f"  Question ID: {qid}\n")

    # ------------------------------------------------------------------
    # YAML merge
    # ------------------------------------------------------------------

    @staticmethod
    def _merge_exclusion_rules(
        new_users: set[str], new_links: set[str]
    ) -> None:
        """Read exclusion_rules.yaml, merge new entries, write back."""
        path = Path(EXCLUSION_RULES_PATH)

        if path.exists():
            with open(path, "r") as f:
                existing = yaml.safe_load(f) or {}
        else:
            existing = {}

        existing_users = set(existing.get("excluded_users", []))
        existing_links = set(existing.get("excluded_message_links", []))

        merged_users = sorted(existing_users | new_users)
        merged_links = sorted(existing_links | new_links)

        added_users = len(merged_users) - len(existing_users)
        added_links = len(merged_links) - len(existing_links)

        data = {
            "excluded_users": merged_users,
            "excluded_message_links": merged_links,
        }
        with open(path, "w") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)

        logger.info(
            "Exclusion rules updated – added %d new users, %d new links "
            "(totals: %d users, %d links)",
            added_users, added_links,
            len(merged_users), len(merged_links),
        )
