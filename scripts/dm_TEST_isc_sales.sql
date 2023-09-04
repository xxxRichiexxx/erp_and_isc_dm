BEGIN TRANSACTION;

DROP TABLE IF EXISTS sttgaz.dm_TEST_isc_sales;
CREATE TABLE sttgaz.dm_TEST_isc_sales AS
WITH 
	sales_agregate AS (
        SELECT
        	DATE_TRUNC('MONTH', "Период")::date         AS "Месяц",
            d."Дивизион",
			s."Внутренний код",
            s."Вариант сборки",
            SUM(s."Продано в розницу")                  AS "Продано в розницу",      
			s."Направление реализации с учетом УКП"
        FROM sttgaz.dds_isc_sales                       AS s
        LEFT JOIN sttgaz.dds_isc_dealer                 AS d
            ON s."Дилер ID" = d.id
        GROUP BY 
        	DATE_TRUNC('MONTH', "Период")::date,
            d."Дивизион",
            s."Внутренний код",
            s."Вариант сборки",
			s."Направление реализации с учетом УКП"
	)
SELECT
	*,
	SUM("Продано в розницу") OVER (
		PARTITION BY Дивизион, "Внутренний код", "Вариант сборки",
		"Направление реализации с учетом УКП" 
		ORDER BY Месяц)	 AS "Продано с накоплением"    
FROM sales_agregate;

GRANT SELECT ON TABLE sttgaz.dm_TEST_isc_sales TO PowerBI_Integration WITH GRANT OPTION;
COMMENT ON TABLE sttgaz.dm_TEST_isc_sales IS 'Продажи ТС из ИСК';

COMMIT TRANSACTION;