BEGIN TRANSACTION;

DROP TABLE IF EXISTS sttgaz.dm_TEST_nom;
CREATE TABLE sttgaz.dm_TEST_nom AS
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
		WHERE n.Дивизион IN ('LCV', 'MCV', 'BUS')
		WINDOW my_window AS (
			PARTITION BY 
			n."Модель на заводе", 
			n.Производитель 
			-- n.Дивизион    ------- Убрать, если появятся дубли и перестанут проходиться проверки.
		)
	)
SELECT DISTINCT
	FIRST_VALUE(Наименование) OVER my_window 								AS Наименование,
	FIRST_VALUE(Код65) OVER my_window 										AS Код65,
	"Модель на заводе",
	Производитель,
	Дивизион
FROM nom_prep
WHERE Код65 IS NOT NULL OR count = 1
WINDOW my_window AS (
PARTITION BY 
	"Модель на заводе",
	Производитель
	-- Дивизион	   ------- Убрать, если появятся дубли и перестанут проходиться проверки.
	ORDER BY Ид DESC);

	
	
DROP TABLE IF EXISTS sttgaz.dm_TEST_erp_sales_v2;
CREATE TABLE sttgaz.dm_TEST_erp_sales_v2 AS
WITH 
	erp_nom_join AS (
			SELECT
			Месяц,
			COALESCE(n.Дивизион, n2.Дивизион) 										AS "Дивизион",
			COALESCE(n.Наименование, n2.Наименование)								AS "Внутренний код",			
			COALESCE(n.Код65, n2.Код65)												AS "ТоварКод65", 
			s."Вариант сборки",
			Реализовано,
			"Направление реализации с учетом УКП"
		FROM sttgaz.dm_erp_kit_sales_v 												AS s
		LEFT JOIN sttgaz.dm_TEST_nom												AS n
			ON s.Контрагент = n.Производитель
				AND(
					"Чертежный номер комплекта" = n."Модель на заводе"
					OR REPLACE(s."Чертежный номер комплекта", '-00', '-') = REGEXP_REPLACE(n.Код65 , '^А', 'A')	    ------- Убрать, если появятся дубли и перестанут проходиться проверки.
					OR REPLACE(s."Чертежный номер комплекта", '-00', '-') = REGEXP_REPLACE(n.Код65 , '^С', 'C')		------- Убрать, если появятся дубли и перестанут проходиться проверки.
				)
		LEFT JOIN sttgaz.dm_TEST_nom												AS n2
			ON n."Модель на заводе" IS NULL AND n2.Производитель ILIKE '%ГАЗ ПАО%'
				AND(
					s."Чертежный номер комплекта" = n2."Модель на заводе"
					OR REPLACE(s."Чертежный номер комплекта", '-00', '-') = REGEXP_REPLACE(n2.Код65 , '^А', 'A')   ------- Убрать, если появятся дубли и перестанут проходиться проверки.
					OR REPLACE(s."Чертежный номер комплекта", '-00', '-') = REGEXP_REPLACE(n2.Код65 , '^С', 'C')   ------- Убрать, если появятся дубли и перестанут проходиться проверки.
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
	),
	calendar AS(
		SELECT DISTINCT DATE_TRUNC('MONTH', ts)::date AS "Месяц"
		FROM (SELECT '2016-01-01'::TIMESTAMP as tm 
			  UNION ALL
			  SELECT NOW() ) as t
		TIMESERIES ts as '1 day' OVER (ORDER BY t.tm)
	),
	units AS(
		SELECT DISTINCT
			"Дивизион",
			"Внутренний код",
			ТоварКод65,
			"Вариант сборки",
			"Направление реализации с учетом УКП" 
		FROM erp_isc_union
	),
	matrix AS(
		SELECT *
		FROM calendar
		CROSS JOIN units
	),
	add_windows AS (
		SELECT
			m."Месяц",
			m."Дивизион",
			m."Внутренний код",
			m.ТоварКод65,
			m."Вариант сборки",
			m."Направление реализации с учетом УКП" ,
			s."Реализовано",
			SUM("Реализовано") OVER (
				PARTITION BY m.Дивизион, m."Внутренний код", m.ТоварКод65, m."Вариант сборки", m."Направление реализации с учетом УКП" 
				ORDER BY m.Месяц
			)		 																										AS "Продано с накоплением",
			'Автокомплекты' 																								AS "Тип продукции"
		FROM matrix																											AS m
		LEFT JOIN erp_isc_union																								AS s
			ON m."Месяц" = s."Месяц"
			AND HASH(
				m."Дивизион",
				m."Внутренний код",
				m.ТоварКод65,
				m."Вариант сборки",
				m."Направление реализации с учетом УКП" 
			) = HASH(
				s."Дивизион",
				s."Внутренний код",
				s.ТоварКод65,
				s."Вариант сборки",
				s."Направление реализации с учетом УКП" 		
			)
	)
SELECT *
FROM add_windows
WHERE "Реализовано" IS NOT NULL 
	OR "Продано с накоплением" IS NOT NULL;

GRANT SELECT ON TABLE sttgaz.dm_TEST_erp_sales_v2 TO PowerBI_Integration WITH GRANT OPTION;
COMMENT ON TABLE sttgaz.dm_TEST_erp_sales_v2 IS 'Продажи автокомплектов из ERP';

DROP TABLE IF EXISTS sttgaz.dm_TEST_nom;

COMMIT TRANSACTION;