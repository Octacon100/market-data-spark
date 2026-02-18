# LinkedIn Post Automation

Prefect 3.0 flow that drafts LinkedIn posts using the Anthropic API and emails
two ready-to-paste variations at 8:15 AM on Tuesday, Wednesday, and Thursday.

The human review step is intentional. The flow handles the creative heavy lifting;
a 60-second copy-paste-edit before posting keeps quality high and avoids the
reputational risk of fully automated social publishing.

---

## Folder Structure

```
linkedin_automation/
  flows/
    draft_generator.py      # Main Prefect flow + scheduling entry point
    email_sender.py         # SMTP email delivery task
  data/
    topics.csv              # Queue of post topics (edit to add your own)
    used_topics.csv         # Tracks which topics have already been used
  config/
    settings.py             # Dataclass config - reads all values from .env
  requirements.txt
  .env.example
  README.md
```

---

## Prerequisites

- Python 3.11
- AWS credentials configured (`aws configure` or IAM role on the host)
- Prefect account (free tier works) or self-hosted Prefect server
- Gmail account with an App Password (2FA must be enabled on the sender account)
- Anthropic API key

---

## Setup

### 1. Install dependencies

Run from the `linkedin_automation/` directory:

```
pip install -r requirements.txt
```

### 2. Configure environment variables

```
cp .env.example .env
```

Edit `.env` and fill in all values. See `.env.example` for field descriptions.

### 3. Upload the seed data to S3

The flow reads topics from S3 so it stays stateless. Upload the CSV files once:

```python
import boto3, os
from dotenv import load_dotenv

load_dotenv()
s3 = boto3.client("s3")
bucket = os.getenv("S3_BUCKET")

s3.upload_file("data/topics.csv",      bucket, "linkedin_automation/data/topics.csv")
s3.upload_file("data/used_topics.csv", bucket, "linkedin_automation/data/used_topics.csv")
```

Or via AWS CLI:

```
aws s3 cp data/topics.csv      s3://<your-bucket>/linkedin_automation/data/topics.csv
aws s3 cp data/used_topics.csv s3://<your-bucket>/linkedin_automation/data/used_topics.csv
```

### 4. Authenticate with Prefect

```
prefect cloud login
```

Or if using a self-hosted server, set `PREFECT_API_URL` in your environment.

---

## Running the Flow

### Run once manually (for testing)

```
python -c "from flows.draft_generator import draft_linkedin_post; draft_linkedin_post()"
```

### Start the scheduler (blocks; runs on cron 15 8 * * 2,3,4)

```
python flows/draft_generator.py
```

This starts `flow.serve()` which blocks the process and dispatches scheduled runs
locally. Keep the terminal open (or run it as a service/screen session).

### Use a Prefect work pool (recommended for production)

```
prefect deployment build flows/draft_generator.py:draft_linkedin_post \
    --name linkedin-post-draft-generator \
    --cron "15 8 * * 2,3,4" \
    --timezone "America/New_York" \
    --pool <your-work-pool>

prefect deployment apply draft_linkedin_post-deployment.yaml
prefect worker start --pool <your-work-pool>
```

---

## Cron Schedule

```
15 8 * * 2,3,4
```

- Minute 15, Hour 8 (8:15 AM)
- Day of week: 2=Tuesday, 3=Wednesday, 4=Thursday
- Timezone: America/New_York

---

## Topic Queue

### Format

`data/topics.csv` columns:

| Column   | Description                         |
|----------|-------------------------------------|
| id       | Unique integer identifier           |
| topic    | The post topic / writing prompt     |
| category | Loose category tag (informational)  |
| notes    | Optional context for the AI         |

### Adding new topics

1. Edit `data/topics.csv` locally and append rows with a new unique `id`.
2. Re-upload to S3:

```
aws s3 cp data/topics.csv s3://<your-bucket>/linkedin_automation/data/topics.csv
```

### Resetting the queue

Delete (or empty) `used_topics.csv` on S3 to start the rotation over:

```
aws s3 cp data/used_topics.csv s3://<your-bucket>/linkedin_automation/data/used_topics.csv
```

When all topics are used the flow automatically resets the queue and cycles from
the beginning, so no intervention is needed for long-running deployments.

---

## Email Format

Each delivery contains both drafts in plain text so you can copy-paste directly
into the LinkedIn post composer. Character counts are included in the email body
because LinkedIn engagement drops sharply above 1,300 characters.

Drafts also appear as Prefect artifacts in the flow run view for archiving.

---

## Gmail App Password Setup

1. Enable 2-Factor Authentication on the sender Gmail account.
2. Go to: Google Account > Security > 2-Step Verification > App passwords.
3. Create a new app password named "LinkedIn Automation".
4. Paste the 16-character password into `EMAIL_PASSWORD` in `.env`.

Do not use your regular Gmail password - Google blocks SMTP login with account
passwords when 2FA is active.

---

## Monitoring

Flow runs and events are visible in Prefect Cloud under the flow name
`linkedin-post-draft-generator`.

Events emitted:
- `linkedin.draft.email.sent` - email delivered successfully
- `linkedin.draft.flow.completed` - full run succeeded
- `linkedin.draft.flow.failed` - any step failed (use this to trigger alerts)

To set up an alert in Prefect Cloud: Automations > Create > Trigger on event
`linkedin.draft.flow.failed`.
