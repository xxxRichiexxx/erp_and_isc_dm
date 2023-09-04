INSERT INTO sttgaz.stage_checks (table_name, check_name, ts, check_result)
WITH sq AS(
    SELECT SUM(s.Реализовано)
    FROM sttgaz.'{{params.dm}}' s
    WHERE EXTRACT(YEAR FROM s.Месяц) = 2021
)
SELECT 
    '{{params.dm}}',
    'comparison_with_target:' || ' 5712=' || (SELECT * FROM sq),
    NOW(),
    5712 = (SELECT * FROM sq);