-- ============================================================
-- Healthcare Claims Data Platform
-- SQL Analytics Queries
-- Gold Layer Reporting Queries
-- ============================================================


-- ============================================================
-- 1. Total Claims
-- Business Question:
-- How many total claims were processed?
-- ============================================================

SELECT
    COUNT(*) AS total_claims
FROM fact_claims;


-- ============================================================
-- 2. Total Claim Amount
-- Business Question:
-- What is the total submitted claim amount?
-- ============================================================

SELECT
    ROUND(SUM(claim_amount), 2) AS total_claim_amount
FROM fact_claims;


-- ============================================================
-- 3. Total Paid Amount
-- Business Question:
-- How much amount was paid?
-- ============================================================

SELECT
    ROUND(SUM(total_paid_amount), 2) AS total_paid_amount
FROM fact_claims;


-- ============================================================
-- 4. Total Balance Amount
-- Business Question:
-- What is the remaining unpaid claim balance?
-- ============================================================

SELECT
    ROUND(SUM(claim_balance_amount), 2) AS total_balance_amount
FROM fact_claims;


-- ============================================================
-- 5. Claim Status Distribution
-- Business Question:
-- How many claims are approved, denied, pending, or submitted?
-- ============================================================

SELECT
    status,
    COUNT(*) AS total_claims,
    ROUND(SUM(claim_amount), 2) AS total_claim_amount
FROM fact_claims
GROUP BY status
ORDER BY total_claims DESC;


-- ============================================================
-- 6. Denial Rate
-- Business Question:
-- What percentage of claims were denied?
-- ============================================================

SELECT
    COUNT(*) AS total_claims,
    SUM(is_denied) AS denied_claims,
    ROUND((SUM(is_denied) * 100.0) / COUNT(*), 2) AS denial_rate_percent
FROM fact_claims;


-- ============================================================
-- 7. Approval Rate
-- Business Question:
-- What percentage of claims were approved?
-- ============================================================

SELECT
    COUNT(*) AS total_claims,
    SUM(is_approved) AS approved_claims,
    ROUND((SUM(is_approved) * 100.0) / COUNT(*), 2) AS approval_rate_percent
FROM fact_claims;


-- ============================================================
-- 8. Monthly Claims Trend
-- Business Question:
-- How are claims trending month by month?
-- ============================================================

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


-- ============================================================
-- 9. Top 10 Providers by Claim Amount
-- Business Question:
-- Which providers have the highest total claim amount?
-- ============================================================

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


-- ============================================================
-- 10. Top 10 Providers by Denial Rate
-- Business Question:
-- Which providers have the highest denial rate?
-- ============================================================

SELECT
    provider_id,
    provider_name,
    specialty,
    provider_state,
    total_claims,
    denied_claims,
    denial_rate_percent
FROM provider_performance_summary
WHERE total_claims >= 10
ORDER BY denial_rate_percent DESC
LIMIT 10;


-- ============================================================
-- 11. Claims by Provider Specialty
-- Business Question:
-- Which specialties process the most claims?
-- ============================================================

SELECT
    specialty,
    COUNT(*) AS total_claims,
    ROUND(SUM(claim_amount), 2) AS total_claim_amount,
    ROUND(SUM(total_paid_amount), 2) AS total_paid_amount,
    ROUND((SUM(is_denied) * 100.0) / COUNT(*), 2) AS denial_rate_percent
FROM fact_claims
GROUP BY specialty
ORDER BY total_claims DESC;


-- ============================================================
-- 12. Claims by Patient State
-- Business Question:
-- Which patient states have the highest claim volume?
-- ============================================================

SELECT
    patient_state,
    COUNT(*) AS total_claims,
    ROUND(SUM(claim_amount), 2) AS total_claim_amount,
    ROUND(SUM(total_paid_amount), 2) AS total_paid_amount
FROM fact_claims
GROUP BY patient_state
ORDER BY total_claims DESC;


-- ============================================================
-- 13. Claims by Provider State
-- Business Question:
-- Which provider states have the highest claim volume?
-- ============================================================

SELECT
    provider_state,
    COUNT(*) AS total_claims,
    ROUND(SUM(claim_amount), 2) AS total_claim_amount,
    ROUND(SUM(total_paid_amount), 2) AS total_paid_amount
FROM fact_claims
GROUP BY provider_state
ORDER BY total_claims DESC;


-- ============================================================
-- 14. Most Common Diagnosis Codes
-- Business Question:
-- Which diagnosis codes appear most often?
-- ============================================================

SELECT
    diagnosis_code,
    COUNT(*) AS total_claims,
    ROUND(SUM(claim_amount), 2) AS total_claim_amount,
    ROUND(SUM(total_paid_amount), 2) AS total_paid_amount
FROM fact_claims
GROUP BY diagnosis_code
ORDER BY total_claims DESC;


-- ============================================================
-- 15. Denied Claims by Diagnosis Code
-- Business Question:
-- Which diagnosis codes have the most denied claims?
-- ============================================================

SELECT
    diagnosis_code,
    COUNT(*) AS denied_claims,
    ROUND(SUM(claim_amount), 2) AS denied_claim_amount
FROM fact_claims
WHERE status = 'DENIED'
GROUP BY diagnosis_code
ORDER BY denied_claims DESC;


-- ============================================================
-- 16. Payment Summary by Claim Status
-- Business Question:
-- How do payments compare across claim statuses?
-- ============================================================

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


-- ============================================================
-- 17. High Value Claims
-- Business Question:
-- Which claims have high claim amounts?
-- ============================================================

SELECT
    claim_id,
    patient_id,
    provider_id,
    provider_name,
    specialty,
    claim_date,
    diagnosis_code,
    procedure_code,
    status,
    claim_amount,
    total_paid_amount,
    claim_balance_amount
FROM fact_claims
WHERE claim_amount >= 4000
ORDER BY claim_amount DESC;


-- ============================================================
-- 18. Unpaid or Low Paid Claims
-- Business Question:
-- Which claims have little or no payment?
-- ============================================================

SELECT
    claim_id,
    provider_name,
    specialty,
    status,
    claim_amount,
    total_paid_amount,
    claim_balance_amount
FROM fact_claims
WHERE total_paid_amount = 0
   OR total_paid_amount < (claim_amount * 0.25)
ORDER BY claim_balance_amount DESC;


-- ============================================================
-- 19. Denial Summary Table
-- Business Question:
-- What denial patterns exist by diagnosis, specialty, and state?
-- ============================================================

SELECT
    diagnosis_code,
    specialty,
    provider_state,
    denied_claims,
    ROUND(denied_claim_amount, 2) AS denied_claim_amount,
    ROUND(avg_denied_claim_amount, 2) AS avg_denied_claim_amount
FROM denial_summary
ORDER BY denied_claims DESC;


-- ============================================================
-- 20. Executive Dashboard KPIs
-- Business Question:
-- What are the top-level dashboard KPIs?
-- ============================================================

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