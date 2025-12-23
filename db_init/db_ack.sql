-- ========================================
-- 第十一部分：初始化验证
-- ========================================

-- 验证用户创建
SELECT 
    User, 
    Host,
    IF(Select_priv = 'Y', '✓', '✗') AS `SELECT权限`,
    IF(Insert_priv = 'Y', '✓', '✗') AS `INSERT权限`,
    IF(Update_priv = 'Y', '✓', '✗') AS `UPDATE权限`,
    IF(Delete_priv = 'Y', '✓', '✗') AS `DELETE权限`
FROM mysql.user 
WHERE User IN ('ingest_user', 'web_user', 'admin_user')
ORDER BY User;

-- 验证表创建
SELECT 
    TABLE_NAME AS '表名',
    ENGINE AS '引擎',
    TABLE_ROWS AS '行数',
    ROUND((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024, 2) AS '大小(MB)',
    TABLE_COMMENT AS '说明'
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = 'ghpulse'
ORDER BY TABLE_NAME;

-- 验证分区
SELECT 
    TABLE_NAME AS '表名',
    PARTITION_NAME AS '分区名',
    PARTITION_METHOD AS '分区方法',
    PARTITION_EXPRESSION AS '分区表达式',
    TABLE_ROWS AS '行数'
FROM information_schema.PARTITIONS
WHERE TABLE_SCHEMA = 'ghpulse' 
  AND PARTITION_NAME IS NOT NULL
ORDER BY TABLE_NAME, PARTITION_ORDINAL_POSITION;

-- 验证索引（仅显示部分关键索引）
SELECT 
    TABLE_NAME AS '表名',
    INDEX_NAME AS '索引名',
    GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) AS '索引列',
    INDEX_TYPE AS '索引类型',
    NON_UNIQUE AS '是否唯一(0=唯一)'
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = 'ghpulse'
  AND TABLE_NAME IN ('actors', 'repos', 'events')
GROUP BY TABLE_NAME, INDEX_NAME, INDEX_TYPE, NON_UNIQUE
ORDER BY TABLE_NAME, INDEX_NAME;

-- 验证存储过程
SELECT 
    ROUTINE_NAME AS '存储过程名',
    ROUTINE_TYPE AS '类型',
    CREATED AS '创建时间',
    DEFINER AS '创建者'
FROM information_schema.ROUTINES
WHERE ROUTINE_SCHEMA = 'ghpulse'
ORDER BY ROUTINE_NAME;

-- 验证触发器
SELECT 
    TRIGGER_NAME AS '触发器名',
    EVENT_MANIPULATION AS '事件',
    EVENT_OBJECT_TABLE AS '目标表',
    ACTION_TIMING AS '时机'
FROM information_schema.TRIGGERS
WHERE TRIGGER_SCHEMA = 'ghpulse'
ORDER BY TRIGGER_NAME;

-- 验证视图
SELECT 
    TABLE_NAME AS '视图名',
    CHECK_OPTION AS '检查选项',
    IS_UPDATABLE AS '可更新'
FROM information_schema.VIEWS
WHERE TABLE_SCHEMA = 'ghpulse'
ORDER BY TABLE_NAME;

-- ========================================
-- 完成
-- ========================================

SELECT '========================================' AS '';
SELECT 'GHPulse 数据库初始化完成！（分区表优化版）' AS '状态';
SELECT '========================================' AS '';
SELECT '' AS '';
SELECT '重要说明：' AS '';
SELECT '1. events表和event_stats_daily表使用分区，已移除外键约束' AS '';
SELECT '2. 数据完整性通过触发器trg_validate_event_insert保证' AS '';
SELECT '3. 提供sp_validate_event_data存储过程用于应用层验证' AS '';
SELECT '4. 非分区表（如user_repo_relation等）保留外键约束' AS '';
SELECT '' AS '';
SELECT '数据库统计：' AS ''; 
SELECT CONCAT('共创建 ', COUNT(*), ' 张表') AS '表数量'
FROM information_schema.TABLES 
WHERE TABLE_SCHEMA = 'ghpulse';

SELECT CONCAT('共创建 ', COUNT(*), ' 个索引') AS '索引数量'
FROM information_schema.STATISTICS 
WHERE TABLE_SCHEMA = 'ghpulse';

SELECT CONCAT('共创建 ', COUNT(*), ' 个存储过程') AS '存储过程数量'
FROM information_schema.ROUTINES 
WHERE ROUTINE_SCHEMA = 'ghpulse';

SELECT CONCAT('共创建 ', COUNT(*), ' 个触发器') AS '触发器数量'
FROM information_schema.TRIGGERS 
WHERE TRIGGER_SCHEMA = 'ghpulse';

SELECT CONCAT('共创建 ', COUNT(*), ' 个视图') AS '视图数量'
FROM information_schema.VIEWS 
WHERE TABLE_SCHEMA = 'ghpulse';

SELECT CONCAT('共创建 ', COUNT(*), ' 个用户') AS '用户数量'
FROM mysql.user 
WHERE User IN ('ingest_user', 'web_user', 'admin_user');

SELECT '' AS '';
SELECT '用户权限说明：' AS '';
SELECT 'ingest_user - 数据写入用户（SELECT, INSERT, UPDATE, DELETE + 存储过程）' AS '';
SELECT 'web_user     - Web只读用户（仅SELECT）' AS '';
SELECT 'admin_user   - 管理员用户（完全权限）' AS '';
SELECT '' AS '';
SELECT '建议定期执行维护任务：' AS '';
SELECT '1. CALL sp_generate_daily_stats(CURDATE());' AS '';
SELECT '2. CALL sp_update_hot_repos();' AS '';
SELECT '3. CALL sp_update_active_developers();' AS '';
SELECT '4. CALL sp_cleanup_old_data(90); -- 保留90天数据' AS '';
SELECT '========================================' AS '';