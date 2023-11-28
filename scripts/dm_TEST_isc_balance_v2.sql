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
 	s.Дивизион,
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
LEFT JOIN	nom n ON s."Внутренний код" = n.Наименование 
GROUP BY 
  	(date_trunc('MONTH'::varchar(5), s.Период))::date,
	s.Дивизион,
    s."Внутренний код",
    n.Код65,
    s."Вариант сборки",
	s."Направление реализации с учетом УКП",
	"Тип продукции";
 
GRANT SELECT ON TABLE sttgaz.dm_TEST_isc_balance_v2 TO PowerBI_Integration WITH GRANT OPTION;
COMMENT ON TABLE sttgaz.dm_TEST_isc_balance_v2 IS 'Остатки готовых ТС из ИСК';

COMMIT TRANSACTION;