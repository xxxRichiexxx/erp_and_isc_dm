BEGIN TRANSACTION;

DROP TABLE IF EXISTS sttgaz.dm_TEST_erp_sales;
CREATE TABLE sttgaz.dm_TEST_erp_sales AS
WITH
sq1 AS(
	SELECT
		Месяц,
		COALESCE(s.Дивизион, n.Дивизион) 										AS "Дивизион",
		n."Наименование"														AS "Внутренний код",
		n.Код65																	AS "ТоварКод65", 
		"Вариант сборки",
		Реализовано,
		"Направление реализации с учетом УКП"
	FROM sttgaz.dm_erp_kit_sales_v 												AS s
	LEFT JOIN sttgaz.dds_isc_nomenclature_guide n
		ON (s."Чертежный номер комплекта" = n."Модель на заводе"
				OR REPLACE(s."Чертежный номер комплекта", '-00', '-')= REGEXP_REPLACE(n.Код65, '^А', 'A')
				OR REPLACE(s."Чертежный номер комплекта", '-00', '-') = REGEXP_REPLACE(n.Код65, '^С', 'C')
			AND n."Модель на заводе" <> '')
			AND UPPER(REPLACE(n."Производитель", ' ', '')) = 'ГАЗПАО'
			AND n."Наименование" <>'Комплект автомобил'
),
sq2 AS(
	SELECT
		Месяц,
		"Дивизион",
		"Внутренний код",
		ТоварКод65,
		"Вариант сборки",
		SUM(Реализовано) 														AS	"Реализовано",
		"Направление реализации с учетом УКП"
	FROM sq1 												
	GROUP BY
		Месяц,
		"Дивизион",
		"Внутренний код",
		ТоварКод65,
		"Вариант сборки",
		"Направление реализации с учетом УКП"
	UNION ALL
	SELECT
			r.Период								                            AS "Месяц",
			div.Наименование						                            AS "Дивизион",
			p.Товар									                            AS "Внутренний код",
			p.ТоварКод65, 
			p."Вариант сборки"						                            AS "Вариант сборки",
			SUM(r.Наличие)							                            AS "Реализовано",
			d."Направление реализации с учетом УКП"                             AS "Направление реализации с учетом УКП" 	
	FROM sttgaz.dds_isc_realization 			                                AS r 
			LEFT JOIN sttgaz.dds_isc_product 		                            AS p
				ON r."Продукт ID" = p.id 
			LEFT JOIN sttgaz.dds_isc_DirectionOfImplementationWithUKP           AS d
				ON r."Направление реализации с учетом УКП ID" = d.id
			LEFT JOIN sttgaz.dds_isc_division  		                            AS div
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
	)		 																	AS "Продано с накоплением" 	
FROM sq2;

GRANT SELECT ON TABLE sttgaz.dm_TEST_erp_sales TO PowerBI_Integration WITH GRANT OPTION;
COMMENT ON TABLE sttgaz.dm_TEST_erp_sales IS 'Продажи автокомплектов из ERP и ИСК';

COMMIT TRANSACTION;