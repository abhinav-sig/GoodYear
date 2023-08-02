With temp as(
    Select TRY_CAST(avg(YEAR_OF_BIRTH) AS FLOAT) yob FROM GOOD_YEAR.RAW.CRM 
)
SELECT MSISDN, 
    CASE 
        WHEN editdistance('FEMALE',upper(GENDER))>=6 and editdistance('MALE',upper(GENDER))>=4 THEN 'UNKNOWN'
        WHEN editdistance('FEMALE',upper(GENDER))<editdistance('MALE',upper(GENDER)) THEN 'FEMALE'
        else 'MALE' END AS GENDER,
    CAST(COALESCE(YEAR_OF_BIRTH, yob
    ) AS NUMERIC) AS YEAR_OF_BIRTH,
    -- YEAR_OF_BIRTH,
    SYSTEM_STATUS,
    MOBILE_TYPE,
    VALUE_SEGMENT
FROM GOOD_YEAR.RAW.CRM, temp