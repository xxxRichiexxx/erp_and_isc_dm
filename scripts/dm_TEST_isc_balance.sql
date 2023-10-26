BEGIN TRANSACTION;

DROP TABLE IF EXISTS sttgaz.dm_TEST_isc_balance;
CREATE TABLE sttgaz.dm_TEST_isc_balance AS
 	SELECT 
 		(date_trunc('MONTH'::varchar(5), s.Период))::date           AS "Месяц",
 		s.Дивизион,
        s."Внутренний код",
        s."Вариант сборки",
        s."Направление реализации с учетом УКП",
        SUM(s."Остатки на НП в пути")								AS "Остатки на НП",
        SUM(s."Остатки на КП в пути")								AS "Остатки на КП"
 	FROM sttgaz.dds_isc_sales 								        AS s 
  	GROUP BY 
  		(date_trunc('MONTH'::varchar(5), s.Период))::date,
        s.Дивизион,
        s."Внутренний код",
        s."Вариант сборки",
		s."Направление реализации с учетом УКП";

 
GRANT SELECT ON TABLE sttgaz.dm_TEST_isc_balance TO PowerBI_Integration WITH GRANT OPTION;
COMMENT ON TABLE sttgaz.dm_TEST_isc_balance IS 'Остатки готовых ТС из ИСК';

COMMIT TRANSACTION;
