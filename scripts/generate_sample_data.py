import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta
import os

fake = Faker()

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")

os.makedirs(RAW_DIR, exist_ok=True)


def random_date(start_days_ago=365, end_days_ago=0):
    start_date = datetime.today() - timedelta(days=start_days_ago)
    end_date = datetime.today() - timedelta(days=end_days_ago)
    return fake.date_between(start_date=start_date, end_date=end_date)


def generate_patients(total_patients=1000):
    patients = []

    for i in range(1, total_patients + 1):
        patients.append({
            "patient_id": f"P{i:05d}",
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "dob": fake.date_of_birth(minimum_age=18, maximum_age=90),
            "gender": random.choice(["M", "F"]),
            "state": random.choice(["TX", "CA", "FL", "NY", "IL", "AZ", "GA"]),
            "created_date": random_date(1000, 0)
        })

    df = pd.DataFrame(patients)
    df.to_csv(os.path.join(RAW_DIR, "patients.csv"), index=False)
    print(f"Generated patients.csv with {len(df)} records")


def generate_providers(total_providers=100):
    specialties = [
        "Cardiology",
        "Orthopedics",
        "Primary Care",
        "Radiology",
        "Dermatology",
        "Neurology",
        "Oncology"
    ]

    providers = []

    for i in range(1, total_providers + 1):
        providers.append({
            "provider_id": f"PR{i:04d}",
            "provider_name": fake.company(),
            "specialty": random.choice(specialties),
            "state": random.choice(["TX", "CA", "FL", "NY", "IL", "AZ", "GA"]),
            "npi": fake.numerify(text="##########")
        })

    df = pd.DataFrame(providers)
    df.to_csv(os.path.join(RAW_DIR, "providers.csv"), index=False)
    print(f"Generated providers.csv with {len(df)} records")


def generate_claims(total_claims=5000, total_patients=1000, total_providers=100):
    diagnosis_codes = ["E11.9", "I10", "M54.5", "J06.9", "R51", "K21.9", "N39.0"]
    procedure_codes = ["99213", "99214", "93000", "80053", "71046", "36415", "97110"]
    statuses = ["APPROVED", "DENIED", "PENDING", "SUBMITTED"]

    claims = []

    for i in range(1, total_claims + 1):
        claim_amount = round(random.uniform(100, 5000), 2)

        claims.append({
            "claim_id": f"C{i:06d}",
            "patient_id": f"P{random.randint(1, total_patients):05d}",
            "provider_id": f"PR{random.randint(1, total_providers):04d}",
            "claim_date": random_date(365, 0),
            "diagnosis_code": random.choice(diagnosis_codes),
            "procedure_code": random.choice(procedure_codes),
            "claim_amount": claim_amount,
            "status": random.choice(statuses),
            "created_timestamp": datetime.now()
        })

    # Add some bad records for data quality testing
    claims.append({
        "claim_id": None,
        "patient_id": "P00001",
        "provider_id": "PR0001",
        "claim_date": datetime.today().date(),
        "diagnosis_code": "E11.9",
        "procedure_code": "99213",
        "claim_amount": 500,
        "status": "APPROVED",
        "created_timestamp": datetime.now()
    })

    claims.append({
        "claim_id": "C_BAD001",
        "patient_id": "P00002",
        "provider_id": "PR0002",
        "claim_date": datetime.today().date(),
        "diagnosis_code": "I10",
        "procedure_code": "99214",
        "claim_amount": -200,
        "status": "APPROVED",
        "created_timestamp": datetime.now()
    })

    claims.append({
        "claim_id": "C_BAD002",
        "patient_id": "P00003",
        "provider_id": "PR0003",
        "claim_date": datetime.today().date(),
        "diagnosis_code": "M54.5",
        "procedure_code": "99214",
        "claim_amount": 700,
        "status": "INVALID_STATUS",
        "created_timestamp": datetime.now()
    })

    df = pd.DataFrame(claims)
    df.to_csv(os.path.join(RAW_DIR, "claims.csv"), index=False)
    print(f"Generated claims.csv with {len(df)} records")


def generate_payments(total_payments=4000, total_claims=5000):
    payment_methods = ["EFT", "CHECK", "CARD", "NONE"]

    payments = []

    for i in range(1, total_payments + 1):
        claim_id = f"C{random.randint(1, total_claims):06d}"
        paid_amount = round(random.uniform(50, 4500), 2)

        payments.append({
            "payment_id": f"PAY{i:06d}",
            "claim_id": claim_id,
            "payment_date": random_date(300, 0),
            "paid_amount": paid_amount,
            "payment_method": random.choice(payment_methods)
        })

    df = pd.DataFrame(payments)
    df.to_csv(os.path.join(RAW_DIR, "payments.csv"), index=False)
    print(f"Generated payments.csv with {len(df)} records")


def generate_claim_status_history(total_claims=5000):
    history = []

    for i in range(1, total_claims + 1):
        claim_id = f"C{i:06d}"

        submitted_time = datetime.now() - timedelta(days=random.randint(1, 365))
        final_status = random.choice(["APPROVED", "DENIED", "PENDING"])

        history.append({
            "claim_id": claim_id,
            "status": "SUBMITTED",
            "updated_timestamp": submitted_time,
            "reason": "Initial claim submission"
        })

        history.append({
            "claim_id": claim_id,
            "status": final_status,
            "updated_timestamp": submitted_time + timedelta(days=random.randint(1, 10)),
            "reason": random.choice([
                "Approved after review",
                "Missing documentation",
                "Invalid diagnosis code",
                "Pending manual review"
            ])
        })

    df = pd.DataFrame(history)
    df.to_csv(os.path.join(RAW_DIR, "claim_status_history.csv"), index=False)
    print(f"Generated claim_status_history.csv with {len(df)} records")


if __name__ == "__main__":
    generate_patients()
    generate_providers()
    generate_claims()
    generate_payments()
    generate_claim_status_history()

    print("All sample healthcare claims data generated successfully.")