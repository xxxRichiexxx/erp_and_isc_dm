	
DROP TABLE IF EXISTS sttgaz.dm_TEST_erp_sales_v2;
CREATE TABLE sttgaz.dm_TEST_erp_sales_v2 AS
WITH 
	nom_prep AS (
		SELECT
			n.Ид ,
			n.Наименование,
			n.Код65,
			n."Модель на заводе",
			n.Производитель,
			n.Дивизион,
			COUNT(*) OVER my_window
		FROM sttgaz.dds_isc_nomenclature_guide n
		WINDOW my_window AS (PARTITION BY n."Модель на заводе", n.Производитель, n.Дивизион)
	),
	nom AS(
		SELECT DISTINCT
			FIRST_VALUE(Наименование) OVER my_window 								AS Наименование,
			FIRST_VALUE(Код65) OVER my_window 										AS Код65,
			"Модель на заводе",
			Производитель,
			Дивизион
		FROM nom_prep
		WHERE Код65 IS NOT NULL OR count = 1
		WINDOW my_window AS (PARTITION BY "Модель на заводе", Производитель, Дивизион ORDER BY Ид DESC)
	),
	erp_nom_join AS(
		SELECT
			Месяц,
			COALESCE(n.Дивизион, n2.Дивизион) 										AS "Дивизион",
			COALESCE(n.Наименование, n2.Наименование)								AS "Внутренний код",			
			COALESCE(n.Код65, n2.Код65)												AS "ТоварКод65", 
			s."Вариант сборки",
			Реализовано,
			"Направление реализации с учетом УКП"
		FROM sttgaz.dm_erp_kit_sales_v 												AS s
		LEFT JOIN nom																AS n
			ON s.Контрагент = n.Производитель
				AND(
					"Чертежный номер комплекта" = n."Модель на заводе"
					OR REPLACE(s."Чертежный номер комплекта", '-00', '-') = REGEXP_REPLACE(n.Код65 , '^А', 'A')
					OR REPLACE(s."Чертежный номер комплекта", '-00', '-') = REGEXP_REPLACE(n.Код65 , '^С', 'C')
				)
		LEFT JOIN nom																AS n2
			ON n."Модель на заводе" IS NULL AND n2.Производитель ILIKE '%ГАЗ ПАО%'
				AND(
					s."Чертежный номер комплекта" = n2."Модель на заводе"
					OR REPLACE(s."Чертежный номер комплекта", '-00', '-') = REGEXP_REPLACE(n2.Код65 , '^А', 'A')
					OR REPLACE(s."Чертежный номер комплекта", '-00', '-') = REGEXP_REPLACE(n2.Код65 , '^С', 'C')
				)
	),
	erp_isc_union AS(
		SELECT
			Месяц,
			"Дивизион",
			"Внутренний код",
			ТоварКод65,
			"Вариант сборки",
			SUM(Реализовано) 														AS	"Реализовано",
			"Направление реализации с учетом УКП"
		FROM erp_nom_join 												
		GROUP BY
			Месяц,
			"Дивизион",
			"Внутренний код",
			ТоварКод65,
			"Вариант сборки",
			"Направление реализации с учетом УКП"
		UNION ALL
		SELECT
				r.Период								AS "Месяц",
				div.Наименование						AS "Дивизион",
				p.Товар									AS "Внутренний код",
				p.ТоварКод65,
				p."Вариант сборки",
				SUM(r.Наличие)							AS "Реализовано",
				d."Направление реализации с учетом УКП" AS "Направление реализации с учетом УКП" 	
		FROM sttgaz.dds_isc_realization 				AS r 
				LEFT JOIN sttgaz.dds_isc_product 		AS p
					ON r."Продукт ID" = p.id 
				LEFT JOIN sttgaz.dds_isc_DirectionOfImplementationWithUKP AS d
					ON r."Направление реализации с учетом УКП ID" = d.id
				LEFT JOIN sttgaz.dds_isc_division  		AS div
					ON p."Дивизион ID"  = div.id
		WHERE DATE_TRUNC('month', r.Период)::date IN ('2022-07-01', '2022-10-01', '2022-12-01', '2023-02-01', '2023-03-01')
					AND div.Наименование = 'LCV'
					AND d."Направление реализации с учетом УКП" = 'СНГ-Казахстан'
		GROUP BY
					r.Период,
					div.Наименование,
					p.Товар,
					p.ТоварКод65,
					p."Вариант сборки",
					d."Направление реализации с учетом УКП"
	)
SELECT
	*,
	SUM("Реализовано") OVER (
		PARTITION BY Дивизион, "Внутренний код", ТоварКод65, "Вариант сборки",
		"Направление реализации с учетом УКП" 
		ORDER BY Месяц
	)		 																										AS "Продано с накоплением",
	'Автокомплекты' 																								AS "Тип продукции"
FROM erp_isc_union;

GRANT SELECT ON TABLE sttgaz.dm_TEST_erp_sales_v2 TO PowerBI_Integration WITH GRANT OPTION;
COMMENT ON TABLE sttgaz.dm_TEST_erp_sales_v2 IS 'Продажи автокомплектов из ERP';



    SELECT SUM(s.Реализовано)
    FROM sttgaz.dm_TEST_erp_sales_v2 s
    WHERE EXTRACT(YEAR FROM s.Месяц) = 2016
    
    
BEGIN TRANSACTION;

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

COMMIT TRANSACTION;


SELECT SUM(s."Продано в розницу")
FROM sttgaz.dm_TEST_isc_sales_v2 s
WHERE EXTRACT(YEAR FROM s.Месяц) = 2019



SELECT SUM(s."Продажи в розницу")
FROM sttgaz.dm_isc_sales_v s
WHERE EXTRACT(YEAR FROM s.Месяц) = 2019


SELECT DISTINCT
	s."Внутренний код"
FROM sttgaz.dm_TEST_erp_sales_v2 s             
WHERE s."Направление реализации с учетом УКП" NOT LIKE 'РФ-%'




BEGIN TRANSACTION;

DROP TABLE IF EXISTS sttgaz.dm_TEST_isc_balance_v2;
CREATE TABLE sttgaz.dm_TEST_isc_balance_v2 AS
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
	)
 SELECT 
 	(date_trunc('MONTH'::varchar(5), s.Период))::date           AS "Месяц",
 	d.Дивизион,
    s."Внутренний код",
    n.Код65,
    s."Вариант сборки",
    s."Направление реализации с учетом УКП",
    SUM(s."Остатки на НП в пути")								AS "Остатки на НП",
  	SUM(s."Остатки на КП в пути")								AS "Остатки на КП",
 	CASE
		WHEN s."Внутренний код" ILIKE '%СемАЗ%' 
			OR s."Внутренний код" ILIKE '%Hazar%'
			OR s."Внутренний код" ILIKE '%Daewoo%'
			--OR s."Внутренний код" ILIKE '%HYUNDAI%'
			OR s."Внутренний код" ILIKE '%HDC%'
			--OR s."Внутренний код" ILIKE '%GAZT%'
			OR s."Внутренний код" ILIKE '%GAZ Cuba%'
			OR s.ВИН ILIKE 'NVB%'
			THEN 'Автокомплекты'
		ELSE 'Собранные ТС'
	END															AS "Тип продукции"	
 FROM sttgaz.dds_isc_sales 								        AS s
 LEFT  JOIN sttgaz.dds_isc_dealer d ON s."Дилер ID" = d.id
 LEFT JOIN	nom n ON s."Внутренний код" = n.Наименование 
 GROUP BY 
  	(date_trunc('MONTH'::varchar(5), s.Период))::date,
	d.Дивизион,
    s."Внутренний код",
    n.Код65,
    s."Вариант сборки",
	s."Направление реализации с учетом УКП",
	"Тип продукции";

 
GRANT SELECT ON TABLE sttgaz.dm_TEST_isc_balance_v2 TO PowerBI_Integration WITH GRANT OPTION;
COMMENT ON TABLE sttgaz.dm_TEST_isc_balance_v2 IS 'Остатки готовых ТС из ИСК';

COMMIT TRANSACTION;


SELECT SUM(s."Остатки на КП в пути")
FROM sttgaz.dds_isc_sales s
WHERE EXTRACT(YEAR FROM s.Период) = 2020

SELECT SUM(s."Остатки на КП")
FROM sttgaz.dm_TEST_isc_balance_v2 s
WHERE EXTRACT(YEAR FROM s.Месяц) = 2020