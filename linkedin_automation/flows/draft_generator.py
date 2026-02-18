"""
LinkedIn Post Draft Generator - Main Prefect Flow.

Reads the next unused topic from S3, calls the Anthropic API to produce two
distinct draft variations, emails them for human review, then marks the topic
as used in S3. Runs on a cron schedule Tue/Wed/Thu at 8:15 AM ET.

Having the human review step baked into the schedule (draft sent morning,
post published at the author's discretion) is intentional - fully automated
LinkedIn publishing carries reputational risk that outweighs the convenience.
"""

import io
import os
import sys
from datetime import datetime
from typing import List, Tuple

import anthropic
import boto3
import pandas as pd
from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from prefect.events import emit_event

# Make the config package importable regardless of which directory the flow
# is invoked from. _ROOT resolves to linkedin_automation/ so 'from config...'
# always finds config/settings.py via the package on sys.path.
_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from config.settings import LinkedInConfig  # noqa: E402
from flows.email_sender import send_draft_email  # noqa: E402

# Instantiated at module level so a misconfigured environment fails immediately
# on import rather than partway through a run - same pattern as market_data_flow.py.
config = LinkedInConfig()
s3_client = boto3.client("s3")
anthropic_client = anthropic.Anthropic(api_key=config.anthropic_api_key)


# ============================================================================
# Prompt Templates
# ============================================================================

SYSTEM_PROMPT = """
You are a ghostwriter for a senior data professional with the following profile:
- Data Architect with over 10 years of experience at hedge funds in Boston
- Deep expertise in AWS (Glue, EMR, Athena, S3), Prefect, Spark, and financial data pipelines
- Known for translating complex technical decisions into clear, practical guidance
- Primary audience: data engineers, architects, and technology leaders in financial services

Writing rules (all are non-negotiable):
- Write entirely in first person, as if the professional is speaking directly
- Tone: professional but conversational - a senior colleague sharing a hard-won lesson
- HARD LIMIT: The full post including hashtags must be under 1,300 characters
- End every post with 3 to 5 hashtags chosen from: #DataEngineering #AWS #FinancialServices #DataArchitecture #CloudInfrastructure #DataGovernance #Prefect #Spark #DataPipelines #HedgeFund
- Open with a specific scenario, a surprising insight, or a counterintuitive lesson - never a generic opener
- Focus on practical takeaways and hard-won experience, not self-promotion or vendor endorsements
- Write in flowing paragraphs, not bullet lists or headers
- Avoid buzzwords and vague corporate language
"""

USER_TEMPLATE = """Generate 2 distinct LinkedIn post drafts about the topic below.
Each draft must take a different angle or opening hook while following all style rules.

Topic: {topic}

Format your response EXACTLY as shown below (include the delimiter lines verbatim):

===DRAFT_1===
[first draft here]
===DRAFT_2===
[second draft here]
===END===
"""


# ============================================================================
# S3 Data Access Tasks
# ============================================================================


@task(
    name="load-topics-from-s3",
    retries=2,
    retry_delay_seconds=30,
    log_prints=True,
    tags=["s3", "topics"],
)
def load_topics_from_s3() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Pull both CSVs from S3 in a single task so all network I/O happens before
    any topic-selection logic runs.

    Storing state in S3 rather than locally keeps the flow stateless - it can
    run from any machine, Prefect worker, or CI job without bootstrapping local
    files. The first time the flow runs, used_topics.csv won't exist yet; that
    is handled gracefully by returning an empty DataFrame.

    Returns:
        Tuple of (all_topics_df, used_topics_df).

    Raises:
        FileNotFoundError: If topics.csv is missing from S3 (setup required).
    """
    print(f"[INFO] Reading topics.csv from s3://{config.s3_bucket}/{config.topics_s3_key}")

    try:
        obj = s3_client.get_object(Bucket=config.s3_bucket, Key=config.topics_s3_key)
        all_topics = pd.read_csv(io.BytesIO(obj["Body"].read()))
    except s3_client.exceptions.NoSuchKey:
        raise FileNotFoundError(
            f"topics.csv not found at s3://{config.s3_bucket}/{config.topics_s3_key}. "
            "Upload the seed file first - see README.md for initial setup instructions."
        )

    print(f"[OK] Loaded {len(all_topics)} total topics")

    # used_topics.csv is absent on the very first run - treat that as an empty
    # usage set so the flow bootstraps itself without manual intervention.
    try:
        obj = s3_client.get_object(
            Bucket=config.s3_bucket, Key=config.used_topics_s3_key
        )
        used_topics = pd.read_csv(io.BytesIO(obj["Body"].read()))
        print(f"[OK] Loaded {len(used_topics)} previously used topics")
    except Exception:
        print("[INFO] used_topics.csv not found on S3 - treating all topics as unused")
        used_topics = pd.DataFrame(
            columns=["id", "topic", "category", "notes", "used_date"]
        )

    return all_topics, used_topics


@task(
    name="pick-next-topic",
    log_prints=True,
    tags=["topics"],
)
def pick_next_topic(
    all_topics: pd.DataFrame,
    used_topics: pd.DataFrame,
) -> pd.Series:
    """
    Return the first topic whose ID does not appear in used_topics.

    Preserving insertion order rather than randomizing gives a predictable
    editorial rotation - useful when the topic queue represents a deliberate
    content sequence. When the queue is exhausted the used list is reset so
    publishing continues; repeating good content after a full cycle is better
    than going silent.

    Args:
        all_topics: Full topics DataFrame from topics.csv.
        used_topics: Previously consumed topics from used_topics.csv.

    Returns:
        A single pd.Series row from all_topics representing the chosen topic.
    """
    used_ids = set(used_topics["id"].tolist()) if not used_topics.empty else set()
    available = all_topics[~all_topics["id"].isin(used_ids)]

    if available.empty:
        # Cycle complete - reset and start over rather than stalling the queue
        print(
            "[WARN] All topics in the queue have been used. "
            "Resetting and cycling from the beginning."
        )
        available = all_topics

    chosen = available.iloc[0]
    print(f"[OK] Selected topic id={chosen['id']}: {chosen['topic']}")
    return chosen


# ============================================================================
# AI Draft Generation Task
# ============================================================================


@task(
    name="generate-linkedin-drafts",
    retries=1,
    retry_delay_seconds=60,
    log_prints=True,
    tags=["anthropic", "ai", "content"],
)
def generate_linkedin_drafts(topic: str) -> List[str]:
    """
    Ask the Anthropic API to produce two distinct draft post variations.

    One API call with structured output delimiters is used rather than two
    separate calls because it halves latency and token cost while still
    yielding meaningfully different drafts when the model is asked to vary
    its angle and hook. One retry covers transient API timeouts without
    creating duplicate email notifications.

    Args:
        topic: The topic string chosen by pick_next_topic.

    Returns:
        List of exactly two draft strings, each ready to paste into LinkedIn.

    Raises:
        ValueError: If the model response cannot be parsed into two drafts.
        anthropic.APIError: Propagates on non-retryable API errors.
    """
    print(f"[INFO] Calling Anthropic API | model={config.anthropic_model}")
    print(f"[INFO] Topic: {topic}")

    message = anthropic_client.messages.create(
        model=config.anthropic_model,
        max_tokens=1500,
        system=SYSTEM_PROMPT.strip(),
        messages=[
            {
                "role": "user",
                "content": USER_TEMPLATE.format(topic=topic),
            }
        ],
    )

    raw_response = message.content[0].text
    print(f"[INFO] Raw API response: {len(raw_response)} chars")

    drafts = _parse_drafts(raw_response)

    for idx, draft in enumerate(drafts, start=1):
        char_count = len(draft)
        status = "[OK]" if char_count <= 1300 else "[WARN]"
        print(f"{status} Draft {idx}: {char_count} chars")
        if char_count > 1300:
            print(
                f"[WARN] Draft {idx} exceeds the 1,300 char sweet spot by "
                f"{char_count - 1300} chars - review before posting"
            )

    return drafts


def _parse_drafts(raw: str) -> List[str]:
    """
    Extract the two draft blocks from the model's structured response.

    The parser is lenient about surrounding whitespace but requires both
    delimiter tags to be present. A missing delimiter is a parse failure
    rather than a silent truncation - better to raise early and retry than
    to email a half-formed draft.

    Args:
        raw: Full text response from the Anthropic API call.

    Returns:
        List of exactly two non-empty draft strings.

    Raises:
        ValueError: If delimiter tags are absent or either parsed block is empty.
    """
    d1_start = raw.find("===DRAFT_1===")
    d2_start = raw.find("===DRAFT_2===")
    end_marker = raw.find("===END===")

    if d1_start == -1 or d2_start == -1:
        raise ValueError(
            "Model response missing DRAFT_1 or DRAFT_2 delimiters. "
            f"First 400 chars of response: {raw[:400]!r}"
        )

    draft_1 = raw[d1_start + len("===DRAFT_1==="):d2_start].strip()

    draft_2_end = end_marker if end_marker != -1 else len(raw)
    draft_2 = raw[d2_start + len("===DRAFT_2==="):draft_2_end].strip()

    if not draft_1 or not draft_2:
        raise ValueError(
            "One or both drafts parsed as empty strings. "
            "Check model response format and delimiter placement."
        )

    return [draft_1, draft_2]


# ============================================================================
# S3 State Write Task
# ============================================================================


@task(
    name="mark-topic-used",
    retries=2,
    retry_delay_seconds=30,
    log_prints=True,
    tags=["s3", "topics"],
)
def mark_topic_used(
    topic_row: pd.Series,
    used_topics: pd.DataFrame,
) -> None:
    """
    Append the consumed topic to used_topics.csv and write the updated file
    back to S3.

    This task runs after a confirmed email send - if email delivery fails the
    topic stays unused so the same draft can be re-sent on the next run rather
    than silently skipping the topic. The tradeoff is a potential duplicate
    email on retry; that is acceptable compared to a silent gap in the queue.

    Args:
        topic_row: The chosen topic row returned by pick_next_topic.
        used_topics: The current used-topics DataFrame to append to.
    """
    new_row = pd.DataFrame(
        [
            {
                "id": topic_row["id"],
                "topic": topic_row["topic"],
                "category": topic_row["category"],
                "notes": topic_row.get("notes", ""),
                "used_date": datetime.now().strftime("%Y-%m-%d"),
            }
        ]
    )

    updated = pd.concat([used_topics, new_row], ignore_index=True)

    csv_buffer = io.StringIO()
    updated.to_csv(csv_buffer, index=False)
    csv_bytes = csv_buffer.getvalue().encode("utf-8")

    s3_client.put_object(
        Bucket=config.s3_bucket,
        Key=config.used_topics_s3_key,
        Body=csv_bytes,
        ContentType="text/csv",
        Metadata={
            "pipeline": "linkedin-automation",
            "last-topic-id": str(topic_row["id"]),
            "updated-at": datetime.now().isoformat(),
        },
    )

    print(
        f"[OK] Marked topic id={topic_row['id']} as used. "
        f"Queue: {len(updated)} used of total topics."
    )
    print(f"[INFO] Updated s3://{config.s3_bucket}/{config.used_topics_s3_key}")


# ============================================================================
# Main Flow
# ============================================================================


@flow(
    name="linkedin-post-draft-generator",
    description="Drafts LinkedIn posts via Anthropic API and emails them for human review",
    log_prints=True,
    retries=0,  # Individual tasks retry; a flow-level retry would re-email duplicates
)
def draft_linkedin_post() -> None:
    """
    Orchestrate the full LinkedIn draft generation pipeline.

    Steps:
    1. Load topics queue and usage history from S3
    2. Pick the next unused topic (resets cycle when queue is exhausted)
    3. Call Anthropic API for two distinct draft variations
    4. Email both drafts to the configured recipient for copy-paste review
    5. Mark the topic as used in S3 (only after confirmed email delivery)
    6. Create a Prefect artifact and emit a success event for monitoring
    """
    run_id = datetime.now().strftime("%Y%m%d-%H%M%S")

    print(f"\n{'=' * 60}")
    print(f"[INFO] LinkedIn Draft Generator | Run: {run_id}")
    print(f"{'=' * 60}\n")

    topic: str = ""
    drafts: List[str] = []

    try:
        # Step 1 - Pull CSVs from S3
        all_topics, used_topics = load_topics_from_s3()

        # Step 2 - Choose topic
        topic_row = pick_next_topic(all_topics, used_topics)
        topic = str(topic_row["topic"])

        # Step 3 - Generate AI drafts
        drafts = generate_linkedin_drafts(topic)

        # Step 4 - Email for human review
        send_draft_email(
            topic=topic,
            drafts=drafts,
            smtp_host=config.smtp_host,
            smtp_port=config.smtp_port,
            email_sender=config.email_sender,
            email_password=config.email_password,
            email_recipient=config.email_recipient,
        )

        # Step 5 - Persist usage state only after email is confirmed sent
        mark_topic_used(topic_row, used_topics)

    except Exception as exc:
        # Emit failure event so Prefect Cloud automations can alert without
        # polling flow run state on a schedule.
        emit_event(
            event="linkedin.draft.flow.failed",
            resource={
                "prefect.resource.id": "linkedin-automation-flow",
                "prefect.resource.name": "LinkedIn Draft Generator",
            },
            payload={
                "run_id": run_id,
                "topic": topic,
                "error": str(exc),
                "error_type": type(exc).__name__,
            },
        )
        print(f"[ERROR] Flow failed: {type(exc).__name__}: {exc}")
        raise

    # Step 6 - Record success
    emit_event(
        event="linkedin.draft.flow.completed",
        resource={
            "prefect.resource.id": "linkedin-automation-flow",
            "prefect.resource.name": "LinkedIn Draft Generator",
        },
        payload={
            "run_id": run_id,
            "topic": topic,
            "draft_1_chars": len(drafts[0]) if drafts else 0,
            "draft_2_chars": len(drafts[1]) if len(drafts) > 1 else 0,
            "recipient": config.email_recipient,
        },
    )

    artifact_md = f"""# LinkedIn Draft Generated

**Run:** {run_id}
**Topic:** {topic}
**Recipient:** {config.email_recipient}

## Draft 1 ({len(drafts[0])} chars)

{drafts[0]}

## Draft 2 ({len(drafts[1]) if len(drafts) > 1 else 0} chars)

{drafts[1] if len(drafts) > 1 else ''}
"""
    create_markdown_artifact(
        key=f"linkedin-draft-{run_id}",
        markdown=artifact_md,
        description=f"LinkedIn drafts for: {topic[:80]}",
    )

    print(f"\n{'=' * 60}")
    print(f"[OK] Flow complete | Topic: {topic}")
    print(f"[OK] Drafts emailed to {config.email_recipient}")
    print(f"{'=' * 60}\n")


# ============================================================================
# Entry Point
# ============================================================================

if __name__ == "__main__":
    # Prefect 3.0 flow.serve() blocks and dispatches scheduled runs locally.
    # For remote or containerized execution, deploy to a Prefect work pool instead.
    draft_linkedin_post.serve(
        name="linkedin-post-draft-generator",
        cron="15 8 * * 2,3,4",
        timezone="America/New_York",
        tags=["linkedin", "content-automation"],
        description="Draft LinkedIn posts Tue/Wed/Thu at 8:15 AM ET",
    )
