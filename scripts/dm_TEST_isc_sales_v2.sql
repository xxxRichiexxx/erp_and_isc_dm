DROP TABLE IF EXISTS sttgaz.dm_TEST_isc_sales_v2;
CREATE TABLE sttgaz.dm_TEST_isc_sales_v2 AS
WITH
	nom_prep AS (
		SELECT
			n.Ид ,
			n.Наименование,
			n.Код65,
			COUNT(*) OVER my_window
		FROM sttgaz.dds_isc_nomenclature_guide n
		WINDOW my_window AS (PARTITION BY n.Наименование)
	),
	nom AS(
		SELECT DISTINCT
			FIRST_VALUE(Код65) OVER my_window 								AS Код65,
			Наименование
		FROM nom_prep
		WHERE Код65 IS NOT NULL OR count = 1
		WINDOW my_window AS (PARTITION BY Наименование ORDER BY Ид DESC)
	),
	sales_agregate AS (
        SELECT
        	DATE_TRUNC('MONTH', "Период")::date         AS "Месяц",
            d."Дивизион",
			s."Внутренний код",
			n.Код65,
            s."Вариант сборки",
            SUM(s."Продано в розницу")                  AS "Продано в розницу",      
			s."Направление реализации с учетом УКП",
			CASE
				WHEN s."Внутренний код" ILIKE '%СемАЗ%' 
					OR s."Внутренний код" ILIKE '%Hazar%'
					OR s."Внутренний код" ILIKE '%Daewoo%'
					--OR s."Внутренний код" ILIKE '%HYUNDAI%'
					OR s."Внутренний код" ILIKE '%HDC%'
					--OR s."Внутренний код" ILIKE '%GAZT%'
					OR s."Внутренний код" ILIKE '%GAZ Cuba%'
					OR s.ВИН ILIKE 'NVB%'
					OR (s."Внутренний код" ILIKE '%С42А43%' and s."Направление реализации с учетом УКП" = 'СНГ-Азербайджан')
					THEN 'Автокомплекты'
				ELSE 'Собранные ТС'
			END											AS "Тип продукции"
        FROM sttgaz.dds_isc_sales                       AS s
        LEFT JOIN sttgaz.dds_isc_dealer                 AS d
            ON s."Дилер ID" = d.id
        LEFT JOIN nom 									AS n 
        	ON s."Внутренний код"  = n.Наименование
        GROUP BY 
        	DATE_TRUNC('MONTH', "Период")::date,
            d."Дивизион",
            s."Внутренний код",
            n.Код65,
            s."Вариант сборки",
			s."Направление реализации с учетом УКП",
			"Тип продукции"
	)
SELECT
	*,
	SUM("Продано в розницу") OVER (
		PARTITION BY Дивизион, "Внутренний код", Код65, "Вариант сборки",
		"Направление реализации с учетом УКП" 
		ORDER BY Месяц)	 AS "Продано с накоплением"    
FROM sales_agregate;

GRANT SELECT ON TABLE sttgaz.dm_TEST_isc_sales_v2 TO PowerBI_Integration WITH GRANT OPTION;
COMMENT ON TABLE sttgaz.dm_TEST_isc_sales_v2 IS 'Продажи ТС из ИСК';