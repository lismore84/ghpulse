-- ========================================
-- 第十部分：用户权限管理
-- ========================================
use ghpulse;
-- 1. 删除可能存在的旧用户
DROP USER IF EXISTS 'ingest_user'@'%';
DROP USER IF EXISTS 'web_user'@'%';
DROP USER IF EXISTS 'admin_user'@'%';

-- 2. 创建数据写入用户（用于ETL脚本）
CREATE USER 'ingest_user'@'%' IDENTIFIED BY 'your_ingest_password';

-- 授予ingest_user权限
GRANT SELECT, INSERT, UPDATE, DELETE ON ghpulse.* TO 'ingest_user'@'%';
GRANT EXECUTE ON PROCEDURE ghpulse.sp_generate_daily_stats TO 'ingest_user'@'%';
GRANT EXECUTE ON PROCEDURE ghpulse.sp_update_hot_repos TO 'ingest_user'@'%';
GRANT EXECUTE ON PROCEDURE ghpulse.sp_update_active_developers TO 'ingest_user'@'%';
GRANT EXECUTE ON PROCEDURE ghpulse.sp_refresh_repo_stats TO 'ingest_user'@'%';
GRANT EXECUTE ON PROCEDURE ghpulse.sp_cleanup_old_data TO 'ingest_user'@'%';
GRANT EXECUTE ON PROCEDURE ghpulse.sp_validate_event_data TO 'ingest_user'@'%';

-- 3. 创建Web只读用户（用于前端展示）
CREATE USER 'web_user'@'%' IDENTIFIED BY 'your_web_password';

-- 授予web_user权限（仅查询）
GRANT SELECT ON ghpulse.* TO 'web_user'@'%';

-- 4. 创建管理员用户（用于数据库维护）
CREATE USER 'admin_user'@'%' IDENTIFIED BY 'your_admin_password';

-- 授予admin_user完全权限
GRANT ALL PRIVILEGES ON ghpulse.* TO 'admin_user'@'%';

-- 5. 刷新权限
FLUSH PRIVILEGES;
