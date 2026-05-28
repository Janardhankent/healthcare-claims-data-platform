import os
import sqlite3
import pandas as pd


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "data", "database", "healthcare_claims.db")


QUERIES = {
    "Executive Dashboard KPIs": """
        SELECT
            COUNT(*) AS total_claims,
            ROUND(SUM(claim_amount), 2) AS total_claim_amount,
            ROUND(SUM(total_paid_amount), 2) AS total_paid_amount,
            ROUND(SUM(claim_balance_amount), 2) AS total_balance_amount,
            SUM(is_denied) AS denied_claims,
            SUM(is_approved) AS approved_claims,
            ROUND((SUM(is_denied) * 100.0) / COUNT(*), 2) AS denial_rate_percent,
            ROUND((SUM(is_approved) * 100.0) / COUNT(*), 2) AS approval_rate_percent
        FROM fact_claims;
    """,

    "Claim Status Distribution": """
        SELECT
            status,
            COUNT(*) AS total_claims,
            ROUND(SUM(claim_amount), 2) AS total_claim_amount
        FROM fact_claims
        GROUP BY status
        ORDER BY total_claims DESC;
    """,

    "Monthly Claims Trend": """
        SELECT
            claim_year,
            claim_month,
            total_claims,
            ROUND(total_claim_amount, 2) AS total_claim_amount,
            ROUND(total_paid_amount, 2) AS total_paid_amount,
            denied_claims,
            approved_claims,
            denial_rate_percent,
            approval_rate_percent
        FROM claim_summary_monthly
        ORDER BY claim_year, claim_month;
    """,

    "Top 10 Providers by Claim Amount": """
        SELECT
            provider_id,
            provider_name,
            specialty,
            provider_state,
            total_claims,
            ROUND(total_claim_amount, 2) AS total_claim_amount,
            ROUND(total_paid_amount, 2) AS total_paid_amount,
            denial_rate_percent
        FROM provider_performance_summary
        ORDER BY total_claim_amount DESC
        LIMIT 10;
    """,

    "Denied Claims by Diagnosis Code": """
        SELECT
            diagnosis_code,
            COUNT(*) AS denied_claims,
            ROUND(SUM(claim_amount), 2) AS denied_claim_amount
        FROM fact_claims
        WHERE status = 'DENIED'
        GROUP BY diagnosis_code
        ORDER BY denied_claims DESC;
    """,

    "Payment Summary by Claim Status": """
        SELECT
            status,
            total_claims,
            ROUND(total_claim_amount, 2) AS total_claim_amount,
            ROUND(total_paid_amount, 2) AS total_paid_amount,
            ROUND(avg_paid_amount, 2) AS avg_paid_amount,
            ROUND(total_balance_amount, 2) AS total_balance_amount,
            payment_rate_percent
        FROM payment_summary
        ORDER BY status;
    """
}


def run_query(connection, query_name, query):
    print("=" * 100)
    print(query_name)
    print("=" * 100)

    df = pd.read_sql_query(query, connection)

    print(df.to_string(index=False))
    print()


def main():
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(
            f"Database not found: {DB_PATH}. Run scripts/load_gold_to_sqlite.py first."
        )

    connection = sqlite3.connect(DB_PATH)

    for query_name, query in QUERIES.items():
        run_query(connection, query_name, query)

    connection.close()


if __name__ == "__main__":
    main()