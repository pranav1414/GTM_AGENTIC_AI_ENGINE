"""
generate_leads.py
=================
Generates 100 realistic synthetic CRM leads and saves to Data/leads.csv
Run from project root: python generate_leads.py
"""

import pandas as pd
import random
from faker import Faker
from pathlib import Path

fake = Faker()
random.seed(42)

INDUSTRIES = [
    "AI/ML", "SaaS", "FinTech", "HealthTech", "EdTech",
    "Cybersecurity", "Data Analytics", "E-commerce", "HRTech", "MarTech"
]

JOB_TITLES = [
    "CEO", "CTO", "VP of Sales", "VP of Marketing", "Director of Operations",
    "Head of Growth", "Chief Revenue Officer", "VP of Engineering",
    "Director of Product", "VP of Customer Success", "COO", "CFO",
    "Head of Data", "Director of Sales", "Senior Manager"
]

LEAD_SOURCES = [
    "Cold Outreach", "Referral", "Inbound", "Event", "LinkedIn",
    "Webinar", "Content Download", "Demo Request", "Partner Referral"
]

DEAL_STAGES = [
    "Prospecting", "Qualification", "Demo Scheduled",
    "Proposal Sent", "Negotiation", "Closed Won", "Closed Lost"
]

COUNTRIES = ["USA", "UK", "Canada", "Australia", "Germany", "India", "Singapore"]

leads = []
for i in range(1, 101):
    industry     = random.choice(INDUSTRIES)
    job_title    = random.choice(JOB_TITLES)
    lead_source  = random.choice(LEAD_SOURCES)
    deal_stage   = random.choice(DEAL_STAGES)
    has_budget   = random.choice([True, False, False])
    in_buying    = random.choice([True, False, False])
    visits       = random.randint(0, 25)
    emails       = random.randint(0, 12)
    demos        = random.randint(0, 4)
    activity     = random.randint(1, 120)
    revenue      = random.choice([
        random.randint(100_000, 999_999),
        random.randint(1_000_000, 9_999_999),
        random.randint(10_000_000, 99_999_999),
    ])
    employees    = random.choice([
        random.randint(5, 49),
        random.randint(50, 499),
        random.randint(500, 5000),
    ])

    leads.append({
        "id":                  i,
        "name":                fake.name(),
        "company":             fake.company(),
        "email":               fake.company_email(),
        "industry":            industry,
        "job_title":           job_title,
        "lead_source":         lead_source,
        "deal_stage":          deal_stage,
        "last_activity_days":  activity,
        "has_budget":          has_budget,
        "in_buying_stage":     in_buying,
        "num_website_visits":  visits,
        "num_emails_opened":   emails,
        "num_demo_requests":   demos,
        "annual_revenue":      revenue,
        "employee_count":      employees,
        "country":             random.choice(COUNTRIES),
        "created_date":        fake.date_between(start_date="-1y", end_date="today").isoformat(),
    })

df = pd.DataFrame(leads)

output_path = Path("Data/leads.csv")
output_path.parent.mkdir(exist_ok=True)
df.to_csv(output_path, index=False)

print(f"Generated {len(df)} leads → {output_path}")
print(f"\nSample breakdown:")
print(f"  Industries:   {df['industry'].value_counts().to_dict()}")
print(f"  Deal stages:  {df['deal_stage'].value_counts().to_dict()}")
print(f"  Has budget:   {df['has_budget'].sum()} leads")
print(f"  Demo requests:{df['num_demo_requests'].sum()} total")