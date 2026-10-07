-- Predefined query for big data extraction
-- Note: Avoid ORDER BY unless necessary, as it slows down large table scans in Oracle.

SELECT 
    id,
    customer_id,
    transaction_date,
    amount,
    status,
    description
FROM 
    large_transactions_table
WHERE 
    transaction_date >= TO_DATE('2025-01-01', 'YYYY-MM-DD')
