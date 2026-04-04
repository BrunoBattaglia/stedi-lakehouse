SELECT 'customer_landing' AS tabela, COUNT(*) AS total FROM stedi.customer_landing
UNION ALL
SELECT 'accelerometer_landing', COUNT(*) FROM stedi.accelerometer_landing
UNION ALL
SELECT 'step_trainer_landing', COUNT(*) FROM stedi.step_trainer_landing
UNION ALL
SELECT 'customer_trusted', COUNT(*) FROM stedi.customer_trusted
UNION ALL
SELECT 'accelerometer_trusted', COUNT(*) FROM stedi.accelerometer_trusted
UNION ALL
SELECT 'step_trainer_trusted', COUNT(*) FROM stedi.step_trainer_trusted
UNION ALL
SELECT 'customer_curated', COUNT(*) FROM stedi.customer_curated_v2
UNION ALL
SELECT 'machine_learning_curated', COUNT(*) FROM stedi.machine_learning_curated;
