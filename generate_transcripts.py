"""
generate_transcripts.py
=======================
Generates realistic Gong-style call transcripts for the first 20 leads
using Gemini 2.5 Flash and saves them as .txt files in Data/
Run from project root: python generate_transcripts.py
"""

import os
import time
import pandas as pd
from pathlib import Path
from google import genai
from dotenv import load_dotenv

load_dotenv()

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
client = genai.Client(api_key=GEMINI_API_KEY)
MODEL  = "gemini-2.5-flash"

LEADS_PATH = Path("Data/leads.csv")
OUTPUT_DIR = Path("Data")
NUM_LEADS  = 20


def generate_transcript(lead: dict) -> str:
    prompt = f"""Generate a realistic 8-10 minute B2B sales discovery call transcript between
a sales rep named Alex and {lead['name']}, who is the {lead['job_title']} at {lead['company']},
a company in the {lead['industry']} industry with approximately {lead['employee_count']} employees.

Call context:
- Lead source: {lead['lead_source']}
- Has budget confirmed: {lead['has_budget']}
- In buying stage: {lead['in_buying_stage']}
- Number of demo requests: {lead['num_demo_requests']}
- Days since last activity: {lead['last_activity_days']}
- Annual revenue: ${lead['annual_revenue']:,}

Make the transcript realistic and natural. Include:
1. Opening and rapport building
2. Discovery questions about their current challenges in {lead['industry']}
3. Discussion of specific pain points
4. Budget and timeline discussion
5. Any objections or concerns they raise
6. Clear next steps

Format strictly as:
Alex: [what alex says]
{lead['name']}: [what the prospect says]

Make it feel like a real conversation. Include specific industry details and clear buying signals or objections based on the context above."""

    response = client.models.generate_content(
        model=MODEL,
        contents=prompt,
    )
    return response.text


def main():
    df    = pd.read_csv(LEADS_PATH)
    leads = df.head(NUM_LEADS).to_dict(orient="records")

    print(f"Generating {NUM_LEADS} Gong-style transcripts using Gemini 2.5 Flash...")
    print("=" * 60)

    success = 0
    for i, lead in enumerate(leads):
        lead_id  = lead["id"]
        filename = OUTPUT_DIR / f"lead_{str(lead_id).zfill(3)}_gong.txt"

        print(f"[{i+1}/{NUM_LEADS}] {lead['name']} — {lead['job_title']} at {lead['company']}...")

        try:
            transcript = generate_transcript(lead)

            header = f"""GONG CALL TRANSCRIPT
====================
Lead ID:    {lead_id}
Contact:    {lead['name']}
Title:      {lead['job_title']}
Company:    {lead['company']}
Industry:   {lead['industry']}
Employees:  {lead['employee_count']}
Revenue:    ${lead['annual_revenue']:,}
Source:     {lead['lead_source']}
Has Budget: {lead['has_budget']}
====================

"""
            with open(filename, "w", encoding="utf-8") as f:
                f.write(header + transcript)

            print(f"  ✅ Saved → {filename}")
            success += 1

        except Exception as e:
            print(f"  ❌ Failed for lead {lead_id}: {e}")

        if i < len(leads) - 1:
            time.sleep(2)

    print("=" * 60)
    print(f"Done. {success}/{NUM_LEADS} transcripts generated → {OUTPUT_DIR}/")


if __name__ == "__main__":
    main()