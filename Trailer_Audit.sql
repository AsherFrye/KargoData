SELECT 
    business_id,
    facility_id,
    facility_name,
    external_id,
    shipment_id,
    created_at,
    purchase_order,
    offender,
    client,
    issue_type,
    price_dollars,
    quantity,
    destination,
    auditor,
    status,
    total_value
FROM (
    SELECT
        f.business_id,
        t.facility_id,
        f.name AS facility_name,
        t.external_id,
        t.truck_shipment_id AS shipment_id,
        t.created_at,
        TRIM(JSON_EXTRACT_SCALAR(t.exception_metadata, '$.purchaseOrder')) AS purchase_order,
        TRIM(JSON_EXTRACT_SCALAR(t.exception_metadata, '$.offender')) AS offender,
        CASE 
            WHEN TRIM(JSON_EXTRACT_SCALAR(t.exception_metadata, '$.client')) IN ('Mcdonalds','MCD') THEN 'McDonalds'
            WHEN TRIM(JSON_EXTRACT_SCALAR(t.exception_metadata, '$.client')) = 'PAN' THEN 'Panera'
            WHEN TRIM(JSON_EXTRACT_SCALAR(t.exception_metadata, '$.client')) = 'DAR' THEN 'Darden'
            ELSE TRIM(JSON_EXTRACT_SCALAR(t.exception_metadata, '$.client'))
        END AS client,
        TRIM(JSON_EXTRACT_SCALAR(t.exception_metadata, '$.OSD')) AS issue_type,
        t.price_dollars,
        t.quantity,
        TRIM(JSON_EXTRACT_SCALAR(t.exception_metadata, '$.destination')) AS destination,
        TRIM(JSON_EXTRACT_SCALAR(t.exception_metadata, '$.auditor')) AS auditor,
        t.status,
        t.price_dollars * t.quantity AS total_value,
        ROW_NUMBER() OVER (PARTITION BY TRIM(JSON_EXTRACT_SCALAR(t.exception_metadata, '$.purchaseOrder')), CAST(t.price_dollars * t.quantity AS STRING) ORDER BY t.created_at DESC) AS rn
    FROM (
        SELECT *,
            CAST(REGEXP_REPLACE(TRIM(JSON_EXTRACT_SCALAR(exception_metadata, '$.price')), r'\D', '') AS INT64) / 100.0 AS price_dollars,
            CAST(REGEXP_REPLACE(TRIM(JSON_EXTRACT_SCALAR(exception_metadata, '$.quantity')), r'\D', '') AS INT64) AS quantity
        FROM `kargo_mysql.truck_shipment_flag`
    ) t
    LEFT JOIN `kargo_mysql.facility` f 
    ON f.id = t.facility_id
    WHERE 
        t.issue_type = 'SHIPMENT_AUDIT' AND 
        t.facility_id IN (SELECT DISTINCT id FROM `test-kargo.kargo_mysql.facility` WHERE business_id = 8) AND t.created_at >= '2023-01-01'
) subquery
WHERE rn = 1
