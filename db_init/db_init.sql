-- ========================================
-- GHPulse 数据库完整设计脚本
-- 适用于：华为云 RDS MySQL 5.7+/8.0+
-- ========================================

-- ========================================
-- 第一部分：数据库初始化
-- ========================================
-- 1. 创建数据库
CREATE DATABASE IF NOT EXISTS ghpulse 
CHARACTER SET utf8mb4 
COLLATE utf8mb4_unicode_ci;
USE ghpulse;

-- 2. 清理现有对象（谨慎使用！）
SET FOREIGN_KEY_CHECKS = 0;

-- 删除触发器
DROP TRIGGER IF EXISTS trg_after_event_insert;
DROP TRIGGER IF EXISTS trg_update_user_repo_relation;
DROP TRIGGER IF EXISTS trg_validate_event_insert;

-- 删除存储过程
DROP PROCEDURE IF EXISTS sp_batch_insert_events;
DROP PROCEDURE IF EXISTS sp_generate_daily_stats;
DROP PROCEDURE IF EXISTS sp_update_hot_repos;
DROP PROCEDURE IF EXISTS sp_update_active_developers;
DROP PROCEDURE IF EXISTS sp_cleanup_old_data;
DROP PROCEDURE IF EXISTS sp_refresh_repo_stats;
DROP PROCEDURE IF EXISTS sp_validate_event_data;

-- 删除视图
DROP VIEW IF EXISTS v_recent_events_summary;
DROP VIEW IF EXISTS v_trending_repos;
DROP VIEW IF EXISTS v_active_developers_leaderboard;
DROP VIEW IF EXISTS v_repo_detail_stats;
DROP VIEW IF EXISTS v_actor_detail_stats;
DROP VIEW IF EXISTS v_daily_event_trends;

-- 删除表（按依赖关系倒序）
DROP TABLE IF EXISTS event_stats_daily;
DROP TABLE IF EXISTS user_repo_relation;
DROP TABLE IF EXISTS actor_stats_cache;
DROP TABLE IF EXISTS repo_stats_cache;
DROP TABLE IF EXISTS active_developers;
DROP TABLE IF EXISTS hot_repos;
DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS payload_delete;
DROP TABLE IF EXISTS payload_create;
DROP TABLE IF EXISTS payload_watch;
DROP TABLE IF EXISTS payload_fork;
DROP TABLE IF EXISTS payload_star;
DROP TABLE IF EXISTS payload_pull_request;
DROP TABLE IF EXISTS payload_issue;
DROP TABLE IF EXISTS payload_push;
DROP TABLE IF EXISTS organizations;
DROP TABLE IF EXISTS repos;
DROP TABLE IF EXISTS actors;

SET FOREIGN_KEY_CHECKS = 1;

-- ========================================
-- 第二部分：基础表创建
-- ========================================

-- 表1：用户表（actors）
CREATE TABLE actors (
    actor_id INT UNSIGNED PRIMARY KEY COMMENT '用户ID（来自GitHub）',
    login VARCHAR(100) NOT NULL COMMENT '用户名',
    display_login VARCHAR(100) NOT NULL COMMENT '显示用户名',
    gravatar_id VARCHAR(100) DEFAULT '' COMMENT 'Gravatar ID',
    url VARCHAR(255) NOT NULL COMMENT '用户主页URL',
    avatar_url VARCHAR(255) NOT NULL COMMENT '头像URL',
    
    -- 统计字段（冗余，提升查询性能）
    total_events INT UNSIGNED DEFAULT 0 COMMENT '总事件数',
    total_repos INT UNSIGNED DEFAULT 0 COMMENT '参与仓库数',
    last_active_at DATETIME COMMENT '最后活跃时间',
    
    -- 时间字段
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    
    -- 索引
    UNIQUE KEY uk_login (login),
    INDEX idx_login (login),
    INDEX idx_last_active (last_active_at),
    INDEX idx_total_events (total_events DESC),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 
  ROW_FORMAT=DYNAMIC 
  COMMENT='GitHub用户信息表';

-- 表2：仓库表（repos）
CREATE TABLE repos (
    repo_id INT UNSIGNED PRIMARY KEY COMMENT '仓库ID（来自GitHub）',
    name VARCHAR(255) NOT NULL COMMENT '仓库全名（owner/repo）',
    url VARCHAR(255) NOT NULL COMMENT '仓库URL',
    
    -- 统计字段
    total_stars INT UNSIGNED DEFAULT 0 COMMENT '总星标数',
    total_forks INT UNSIGNED DEFAULT 0 COMMENT '总Fork数',
    total_events INT UNSIGNED DEFAULT 0 COMMENT '总事件数',
    total_contributors INT UNSIGNED DEFAULT 0 COMMENT '贡献者数',
    last_event_at DATETIME COMMENT '最后事件时间',
    
    -- 分类字段
    language VARCHAR(50) COMMENT '主要编程语言',
    is_hot TINYINT(1) DEFAULT 0 COMMENT '是否热门仓库（0=否，1=是）',
    
    -- 时间字段
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    
    -- 索引
    UNIQUE KEY uk_name (name),
    INDEX idx_repo_name (name),
    INDEX idx_total_stars (total_stars DESC),
    INDEX idx_total_forks (total_forks DESC),
    INDEX idx_last_event (last_event_at),
    INDEX idx_is_hot (is_hot, total_stars DESC),
    INDEX idx_language (language)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 
  ROW_FORMAT=DYNAMIC 
  COMMENT='GitHub仓库信息表';

-- 表3：组织表（organizations）
CREATE TABLE organizations (
    org_id INT UNSIGNED PRIMARY KEY COMMENT '组织ID（来自GitHub）',
    login VARCHAR(100) NOT NULL COMMENT '组织名',
    gravatar_id VARCHAR(100) DEFAULT '' COMMENT 'Gravatar ID',
    url VARCHAR(255) NOT NULL COMMENT '组织主页URL',
    avatar_url VARCHAR(255) NOT NULL COMMENT '组织头像URL',
    
    -- 统计字段
    total_repos INT UNSIGNED DEFAULT 0 COMMENT '组织仓库数',
    total_members INT UNSIGNED DEFAULT 0 COMMENT '组织成员数',
    
    -- 时间字段
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    
    -- 索引
    UNIQUE KEY uk_login (login),
    INDEX idx_org_login (login),
    INDEX idx_total_repos (total_repos DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 
  ROW_FORMAT=DYNAMIC 
  COMMENT='GitHub组织信息表';

-- ========================================
-- 第三部分：Payload载荷表
-- ========================================

-- 表4：PushEvent载荷表
CREATE TABLE payload_push (
    payload_id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT COMMENT '载荷ID（自增主键）',
    push_id BIGINT UNSIGNED COMMENT 'Push事件ID',
    size INT DEFAULT 0 COMMENT '提交数量',
    distinct_size INT DEFAULT 0 COMMENT '不同提交数',
    head VARCHAR(100) COMMENT '最新提交SHA',
    ref VARCHAR(255) COMMENT '引用（分支/标签）',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    
    -- 索引
    INDEX idx_push_id (push_id),
    INDEX idx_push_ref (ref(100)),
    INDEX idx_size (size DESC),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 
  COMMENT='PushEvent载荷表';

-- 表5：IssueEvent载荷表
CREATE TABLE payload_issue (
    payload_id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT COMMENT '载荷ID（自增主键）',
    action VARCHAR(50) NOT NULL COMMENT '动作（opened/closed/reopened等）',
    issue_id BIGINT UNSIGNED NOT NULL COMMENT 'Issue ID',
    issue_number INT UNSIGNED COMMENT 'Issue编号',
    issue_title VARCHAR(500) NOT NULL COMMENT 'Issue标题',
    issue_body TEXT COMMENT 'Issue内容',
    issue_state VARCHAR(20) COMMENT 'Issue状态（open/closed）',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    
    -- 索引
    INDEX idx_issue_id (issue_id),
    INDEX idx_issue_action (action),
    INDEX idx_issue_state (issue_state),
    INDEX idx_issue_number (issue_number),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 
  COMMENT='IssueEvent载荷表';

-- 表6：PullRequestEvent载荷表
CREATE TABLE payload_pull_request (
    payload_id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT COMMENT '载荷ID（自增主键）',
    action VARCHAR(50) NOT NULL COMMENT '动作（opened/closed/merged等）',
    pr_id BIGINT UNSIGNED NOT NULL COMMENT 'PR ID',
    pr_number INT UNSIGNED COMMENT 'PR编号',
    pr_title VARCHAR(500) NOT NULL COMMENT 'PR标题',
    pr_state VARCHAR(20) NOT NULL COMMENT 'PR状态（open/closed）',
    merged TINYINT(1) DEFAULT 0 COMMENT '是否已合并（0=否，1=是）',
    base_ref VARCHAR(255) NOT NULL COMMENT '目标分支',
    head_ref VARCHAR(255) NOT NULL COMMENT '源分支',
    additions INT UNSIGNED DEFAULT 0 COMMENT '添加行数',
    deletions INT UNSIGNED DEFAULT 0 COMMENT '删除行数',
    changed_files INT UNSIGNED DEFAULT 0 COMMENT '变更文件数',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    
    -- 索引
    INDEX idx_pr_id (pr_id),
    INDEX idx_pr_number (pr_number),
    INDEX idx_pr_state (pr_state),
    INDEX idx_pr_action (action),
    INDEX idx_merged (merged),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 
  COMMENT='PullRequestEvent载荷表';

-- 表7：StarEvent载荷表
CREATE TABLE payload_star (
    payload_id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT COMMENT '载荷ID（自增主键）',
    action VARCHAR(20) NOT NULL COMMENT '动作（started）',
    star_repo_id INT UNSIGNED NOT NULL COMMENT '被星标的仓库ID',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    
    -- 索引
    INDEX idx_star_action (action),
    INDEX idx_star_repo (star_repo_id),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 
  COMMENT='WatchEvent(Star)载荷表';

-- 表8：ForkEvent载荷表
CREATE TABLE payload_fork (
    payload_id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT COMMENT '载荷ID（自增主键）',
    forkee_id INT UNSIGNED NOT NULL COMMENT 'Fork后的仓库ID',
    forkee_name VARCHAR(255) NOT NULL COMMENT 'Fork后的仓库全名',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    
    -- 索引
    INDEX idx_forkee_id (forkee_id),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 
  COMMENT='ForkEvent载荷表';

-- 表9：WatchEvent载荷表
CREATE TABLE payload_watch (
    payload_id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT COMMENT '载荷ID（自增主键）',
    action VARCHAR(20) NOT NULL COMMENT '动作（started）',
    watch_repo_id INT UNSIGNED NOT NULL COMMENT '被关注的仓库ID',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    
    -- 索引
    INDEX idx_watch_action (action),
    INDEX idx_watch_repo (watch_repo_id),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 
  COMMENT='WatchEvent载荷表';

-- 表10：CreateEvent载荷表
CREATE TABLE payload_create (
    payload_id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT COMMENT '载荷ID（自增主键）',
    ref VARCHAR(255) COMMENT '引用名称',
    ref_type VARCHAR(20) NOT NULL COMMENT '引用类型（branch/tag/repository）',
    description TEXT COMMENT '描述',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    
    -- 索引
    INDEX idx_ref_type (ref_type),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 
  COMMENT='CreateEvent载荷表';

-- 表11：DeleteEvent载荷表
CREATE TABLE payload_delete (
    payload_id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT COMMENT '载荷ID（自增主键）',
    ref VARCHAR(255) COMMENT '引用名称',
    ref_type VARCHAR(20) NOT NULL COMMENT '引用类型（branch/tag）',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    
    -- 索引
    INDEX idx_ref_type (ref_type),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 
  COMMENT='DeleteEvent载荷表';

-- ========================================
-- 第四部分：事件主表（分区表 - 无外键）
-- ========================================

-- 表12：事件主表（按月分区）
-- 注意：MySQL分区表不支持外键，数据完整性由应用层和触发器保证
CREATE TABLE events (
    event_id BIGINT UNSIGNED AUTO_INCREMENT COMMENT '事件ID（自增主键）',
    gh_event_id BIGINT UNSIGNED NOT NULL COMMENT 'GitHub事件ID',
    event_type VARCHAR(50) NOT NULL COMMENT '事件类型',
    public TINYINT(1) NOT NULL DEFAULT 1 COMMENT '是否公开（0=否，1=是）',
    created_at DATETIME NOT NULL COMMENT '事件发生时间',
    created_at_date DATE NOT NULL COMMENT '事件发生日期（用于分区）',
    
    -- 关联字段（注意：分区表不能使用外键，需在应用层保证数据完整性）
    actor_id INT UNSIGNED NOT NULL COMMENT '用户ID（关联actors.actor_id）',
    repo_id INT UNSIGNED NOT NULL COMMENT '仓库ID（关联repos.repo_id）',
    org_id INT UNSIGNED COMMENT '组织ID（关联organizations.org_id，可为空）',
    payload_id INT UNSIGNED NOT NULL COMMENT '载荷ID',
    
    -- 冗余字段（减少JOIN查询）
    actor_login VARCHAR(100) COMMENT '用户名（冗余）',
    repo_name VARCHAR(255) COMMENT '仓库名（冗余）',
    
    -- 主键和唯一键
    PRIMARY KEY (event_id, created_at_date),
    UNIQUE KEY uk_gh_event_id (gh_event_id, created_at_date),
    
    -- 核心索引
    INDEX idx_event_type (event_type, created_at),
    INDEX idx_created_at (created_at),
    INDEX idx_actor_repo (actor_id, repo_id, created_at),
    INDEX idx_repo_type (repo_id, event_type, created_at),
    INDEX idx_created_date (created_at_date),
    INDEX idx_org_created (org_id, created_at),
    INDEX idx_actor_id (actor_id),
    INDEX idx_repo_id (repo_id),
    
    -- 覆盖索引
    INDEX idx_actor_type_date (actor_id, event_type, created_at_date)
    
    -- 外键约束已移除（分区表限制）
    -- 数据完整性由应用层和触发器保证
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 
  ROW_FORMAT=DYNAMIC 
  COMMENT='GitHub事件主表（分区表，无外键约束）'
PARTITION BY RANGE COLUMNS(created_at_date) (
    PARTITION p202401 VALUES LESS THAN ('2024-02-01') COMMENT '2024年1月',
    PARTITION p202402 VALUES LESS THAN ('2024-03-01') COMMENT '2024年2月',
    PARTITION p202403 VALUES LESS THAN ('2024-04-01') COMMENT '2024年3月',
    PARTITION p202404 VALUES LESS THAN ('2024-05-01') COMMENT '2024年4月',
    PARTITION p202405 VALUES LESS THAN ('2024-06-01') COMMENT '2024年5月',
    PARTITION p202406 VALUES LESS THAN ('2024-07-01') COMMENT '2024年6月',
    PARTITION p202407 VALUES LESS THAN ('2024-08-01') COMMENT '2024年7月',
    PARTITION p202408 VALUES LESS THAN ('2024-09-01') COMMENT '2024年8月',
    PARTITION p202409 VALUES LESS THAN ('2024-10-01') COMMENT '2024年9月',
    PARTITION p202410 VALUES LESS THAN ('2024-11-01') COMMENT '2024年10月',
    PARTITION p202411 VALUES LESS THAN ('2024-12-01') COMMENT '2024年11月',
    PARTITION p202412 VALUES LESS THAN ('2025-01-01') COMMENT '2024年12月',
    PARTITION p202501 VALUES LESS THAN ('2025-02-01') COMMENT '2025年1月',
    PARTITION p202502 VALUES LESS THAN ('2025-03-01') COMMENT '2025年2月',
    PARTITION p202503 VALUES LESS THAN ('2025-04-01') COMMENT '2025年3月',
    PARTITION p202504 VALUES LESS THAN ('2025-05-01') COMMENT '2025年4月',
    PARTITION p202505 VALUES LESS THAN ('2025-06-01') COMMENT '2025年5月',
    PARTITION p202506 VALUES LESS THAN ('2025-07-01') COMMENT '2025年6月',
    PARTITION p_future VALUES LESS THAN MAXVALUE COMMENT '未来数据'
);

-- ========================================
-- 第五部分：关联和统计表
-- ========================================

-- 表13：用户-仓库关联表（非分区，可使用外键）
CREATE TABLE user_repo_relation (
    relation_id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT COMMENT '关联ID（自增主键）',
    actor_id INT UNSIGNED NOT NULL COMMENT '用户ID',
    repo_id INT UNSIGNED NOT NULL COMMENT '仓库ID',
    relation_type VARCHAR(50) NOT NULL COMMENT '关联类型（star/fork/contributor/watcher）',
    relation_time DATETIME NOT NULL COMMENT '建立关联时间',
    first_event_at DATETIME COMMENT '首次事件时间',
    last_event_at DATETIME COMMENT '最后事件时间',
    event_count INT UNSIGNED DEFAULT 0 COMMENT '事件次数',
    is_valid TINYINT(1) NOT NULL DEFAULT 1 COMMENT '是否有效（0=无效，1=有效）',
    
    -- 唯一约束
    UNIQUE KEY uk_actor_repo_type (actor_id, repo_id, relation_type),
    
    -- 索引
    INDEX idx_actor_relation (actor_id, relation_type, is_valid),
    INDEX idx_repo_relation (repo_id, relation_type, is_valid),
    INDEX idx_relation_time (relation_time),
    INDEX idx_last_event (last_event_at),
    
    -- 外键（非分区表可以使用）
    CONSTRAINT fk_relation_actor 
        FOREIGN KEY (actor_id) REFERENCES actors(actor_id) 
        ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT fk_relation_repo 
        FOREIGN KEY (repo_id) REFERENCES repos(repo_id) 
        ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 
  COMMENT='用户-仓库关联表';

-- 表14：每日事件统计表（分区表 - 无外键）
CREATE TABLE event_stats_daily (
    stats_id INT UNSIGNED AUTO_INCREMENT COMMENT '统计ID（自增主键）',
    stats_date DATE NOT NULL COMMENT '统计日期',
    event_type VARCHAR(50) NOT NULL COMMENT '事件类型',
    
    -- 统计指标
    total_count INT UNSIGNED NOT NULL DEFAULT 0 COMMENT '总事件数',
    unique_actors INT UNSIGNED NOT NULL DEFAULT 0 COMMENT '唯一用户数',
    unique_repos INT UNSIGNED NOT NULL DEFAULT 0 COMMENT '唯一仓库数',
    unique_orgs INT UNSIGNED NOT NULL DEFAULT 0 COMMENT '唯一组织数',
    
    -- 时间字段
    stats_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP 
        ON UPDATE CURRENT_TIMESTAMP COMMENT '统计时间',
    
    -- 主键和唯一键
    PRIMARY KEY (stats_id, stats_date),
    UNIQUE KEY uk_date_type (stats_date, event_type),
    
    -- 索引
    INDEX idx_stats_date (stats_date),
    INDEX idx_stats_type (event_type),
    INDEX idx_total_count (total_count DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 
  COMMENT='每日事件统计表（分区表）'
PARTITION BY RANGE COLUMNS(stats_date) (
    PARTITION p202401 VALUES LESS THAN ('2024-02-01') COMMENT '2024年1月',
    PARTITION p202402 VALUES LESS THAN ('2024-03-01') COMMENT '2024年2月',
    PARTITION p202403 VALUES LESS THAN ('2024-04-01') COMMENT '2024年3月',
    PARTITION p202404 VALUES LESS THAN ('2024-05-01') COMMENT '2024年4月',
    PARTITION p202405 VALUES LESS THAN ('2024-06-01') COMMENT '2024年5月',
    PARTITION p202406 VALUES LESS THAN ('2024-07-01') COMMENT '2024年6月',
    PARTITION p202407 VALUES LESS THAN ('2024-08-01') COMMENT '2024年7月',
    PARTITION p202408 VALUES LESS THAN ('2024-09-01') COMMENT '2024年8月',
    PARTITION p202409 VALUES LESS THAN ('2024-10-01') COMMENT '2024年9月',
    PARTITION p202410 VALUES LESS THAN ('2024-11-01') COMMENT '2024年10月',
    PARTITION p202411 VALUES LESS THAN ('2024-12-01') COMMENT '2024年11月',
    PARTITION p202412 VALUES LESS THAN ('2025-01-01') COMMENT '2024年12月',
    PARTITION p202501 VALUES LESS THAN ('2025-02-01') COMMENT '2025年1月',
    PARTITION p202502 VALUES LESS THAN ('2025-03-01') COMMENT '2025年2月',
    PARTITION p202503 VALUES LESS THAN ('2025-04-01') COMMENT '2025年3月',
    PARTITION p_future VALUES LESS THAN MAXVALUE COMMENT '未来数据'
);

-- ========================================
-- 第六部分：缓存表（加速查询）
-- ========================================

-- 表15：热门仓库缓存表（非分区，可使用外键）
CREATE TABLE hot_repos (
    repo_id INT UNSIGNED PRIMARY KEY COMMENT '仓库ID',
    repo_name VARCHAR(255) NOT NULL COMMENT '仓库名',
    score DECIMAL(10,2) NOT NULL COMMENT '热度分数',
    stars_7d INT UNSIGNED DEFAULT 0 COMMENT '7天新增星标',
    forks_7d INT UNSIGNED DEFAULT 0 COMMENT '7天新增fork',
    prs_7d INT UNSIGNED DEFAULT 0 COMMENT '7天PR数',
    contributors_7d INT UNSIGNED DEFAULT 0 COMMENT '7天活跃贡献者',
    rank_position INT UNSIGNED COMMENT '排名位置',
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP 
        ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    
    -- 索引
    INDEX idx_score (score DESC),
    INDEX idx_rank (rank_position),
    INDEX idx_stars_7d (stars_7d DESC),
    INDEX idx_updated (updated_at),
    
    -- 外键（非分区表可以使用）
    CONSTRAINT fk_hot_repos 
        FOREIGN KEY (repo_id) REFERENCES repos(repo_id) 
        ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 
  COMMENT='热门仓库缓存表';

-- 表16：活跃开发者缓存表（非分区，可使用外键）
CREATE TABLE active_developers (
    actor_id INT UNSIGNED PRIMARY KEY COMMENT '用户ID',
    actor_login VARCHAR(100) NOT NULL COMMENT '用户名',
    activity_score DECIMAL(10,2) NOT NULL COMMENT '活跃度分数',
    commits_7d INT UNSIGNED DEFAULT 0 COMMENT '7天提交数',
    prs_7d INT UNSIGNED DEFAULT 0 COMMENT '7天PR数',
    issues_7d INT UNSIGNED DEFAULT 0 COMMENT '7天Issue数',
    repos_7d INT UNSIGNED DEFAULT 0 COMMENT '7天参与仓库数',
    rank_position INT UNSIGNED COMMENT '排名位置',
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP 
        ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    
    -- 索引
    INDEX idx_score (activity_score DESC),
    INDEX idx_rank (rank_position),
    INDEX idx_commits (commits_7d DESC),
    INDEX idx_updated (updated_at),
    
    -- 外键（非分区表可以使用）
    CONSTRAINT fk_active_developers 
        FOREIGN KEY (actor_id) REFERENCES actors(actor_id) 
        ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 
  COMMENT='活跃开发者缓存表';

-- 表17：仓库统计缓存表（非分区，可使用外键）
CREATE TABLE repo_stats_cache (
    repo_id INT UNSIGNED PRIMARY KEY COMMENT '仓库ID',
    total_stars INT UNSIGNED DEFAULT 0 COMMENT '总星标数',
    total_forks INT UNSIGNED DEFAULT 0 COMMENT '总Fork数',
    total_watchers INT UNSIGNED DEFAULT 0 COMMENT '总关注数',
    total_contributors INT UNSIGNED DEFAULT 0 COMMENT '总贡献者数',
    total_commits INT UNSIGNED DEFAULT 0 COMMENT '总提交数',
    total_prs INT UNSIGNED DEFAULT 0 COMMENT '总PR数',
    total_issues INT UNSIGNED DEFAULT 0 COMMENT '总Issue数',
    
    -- 近期统计
    stars_1d INT UNSIGNED DEFAULT 0 COMMENT '1天新增星标',
    stars_7d INT UNSIGNED DEFAULT 0 COMMENT '7天新增星标',
    stars_30d INT UNSIGNED DEFAULT 0 COMMENT '30天新增星标',
    
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP 
        ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    
    -- 索引
    INDEX idx_stars (total_stars DESC),
    INDEX idx_stars_7d (stars_7d DESC),
    INDEX idx_updated (updated_at),
    
    -- 外键（非分区表可以使用）
    CONSTRAINT fk_repo_stats 
        FOREIGN KEY (repo_id) REFERENCES repos(repo_id) 
        ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 
  COMMENT='仓库统计缓存表';

-- 表18：用户统计缓存表（非分区，可使用外键）
CREATE TABLE actor_stats_cache (
    actor_id INT UNSIGNED PRIMARY KEY COMMENT '用户ID',
    total_commits INT UNSIGNED DEFAULT 0 COMMENT '总提交数',
    total_prs INT UNSIGNED DEFAULT 0 COMMENT '总PR数',
    total_issues INT UNSIGNED DEFAULT 0 COMMENT '总Issue数',
    total_repos INT UNSIGNED DEFAULT 0 COMMENT '总参与仓库数',
    total_stars_received INT UNSIGNED DEFAULT 0 COMMENT '获得的总星标数',
    
    -- 近期统计
    commits_7d INT UNSIGNED DEFAULT 0 COMMENT '7天提交数',
    prs_7d INT UNSIGNED DEFAULT 0 COMMENT '7天PR数',
    repos_7d INT UNSIGNED DEFAULT 0 COMMENT '7天参与仓库数',
    
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP 
        ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    
    -- 索引
    INDEX idx_commits (total_commits DESC),
    INDEX idx_stars (total_stars_received DESC),
    INDEX idx_updated (updated_at),
    
    -- 外键（非分区表可以使用）
    CONSTRAINT fk_actor_stats 
        FOREIGN KEY (actor_id) REFERENCES actors(actor_id) 
        ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 
  COMMENT='用户统计缓存表';

-- ========================================
-- 第七部分：存储过程
-- ========================================

DELIMITER //

-- 存储过程0：数据完整性验证（补偿外键约束）
CREATE PROCEDURE sp_validate_event_data(
    IN p_actor_id INT UNSIGNED,
    IN p_repo_id INT UNSIGNED,
    IN p_org_id INT UNSIGNED,
    OUT p_valid TINYINT(1),
    OUT p_error_msg VARCHAR(255)
)
main_block: BEGIN
    DECLARE v_actor_exists INT DEFAULT 0;
    DECLARE v_repo_exists INT DEFAULT 0;
    DECLARE v_org_exists INT DEFAULT 0;
    
    SET p_valid = 1;
    SET p_error_msg = '';
    
    -- 验证actor_id
    SELECT COUNT(*) INTO v_actor_exists FROM actors WHERE actor_id = p_actor_id;
    IF v_actor_exists = 0 THEN
        SET p_valid = 0;
        SET p_error_msg = CONCAT('Invalid actor_id: ', p_actor_id);
        LEAVE main_block;
    END IF;
    
    -- 验证repo_id
    SELECT COUNT(*) INTO v_repo_exists FROM repos WHERE repo_id = p_repo_id;
    IF v_repo_exists = 0 THEN
        SET p_valid = 0;
        SET p_error_msg = CONCAT('Invalid repo_id: ', p_repo_id);
        LEAVE main_block;
    END IF;
    
    -- 验证org_id（可为空）
    IF p_org_id IS NOT NULL THEN
        SELECT COUNT(*) INTO v_org_exists FROM organizations WHERE org_id = p_org_id;
        IF v_org_exists = 0 THEN
            SET p_valid = 0;
            SET p_error_msg = CONCAT('Invalid org_id: ', p_org_id);
            LEAVE main_block;
        END IF;
    END IF;
END main_block //

-- 存储过程1：生成每日统计
CREATE PROCEDURE sp_generate_daily_stats(
    IN p_target_date DATE
)
BEGIN
    DECLARE v_start_time DATETIME DEFAULT NOW();
    DECLARE v_rows_affected INT DEFAULT 0;
    
    -- 删除当天旧数据
    DELETE FROM event_stats_daily WHERE stats_date = p_target_date;
    
    -- 生成新统计
    INSERT INTO event_stats_daily (
        stats_date, 
        event_type, 
        total_count, 
        unique_actors, 
        unique_repos, 
        unique_orgs
    )
    SELECT 
        p_target_date AS stats_date,
        event_type,
        COUNT(*) AS total_count,
        COUNT(DISTINCT actor_id) AS unique_actors,
        COUNT(DISTINCT repo_id) AS unique_repos,
        COUNT(DISTINCT org_id) AS unique_orgs
    FROM events
    WHERE created_at_date = p_target_date
    GROUP BY event_type;
    
    SET v_rows_affected = ROW_COUNT();
    
    SELECT CONCAT('统计生成完成: ', p_target_date, 
                  ', 共', v_rows_affected, '条记录, ',
                  '耗时', TIMESTAMPDIFF(SECOND, v_start_time, NOW()), '秒') 
           AS message;
END //

-- 存储过程2：更新热门仓库榜单
CREATE PROCEDURE sp_update_hot_repos()
BEGIN
    DECLARE v_start_date DATE DEFAULT DATE_SUB(CURDATE(), INTERVAL 7 DAY);
    DECLARE v_start_time DATETIME DEFAULT NOW();
    
    -- 清空旧数据
    TRUNCATE TABLE hot_repos;
    
    -- 生成新榜单
    INSERT INTO hot_repos (
        repo_id, 
        repo_name, 
        score, 
        stars_7d, 
        forks_7d, 
        prs_7d, 
        contributors_7d,
        rank_position
    )
    SELECT 
        r.repo_id,
        r.name AS repo_name,
        -- 计算综合分数：星标*3 + fork*2 + PR*1.5 + 贡献者*2
        (COALESCE(stars.cnt, 0) * 3 + 
         COALESCE(forks.cnt, 0) * 2 + 
         COALESCE(prs.cnt, 0) * 1.5 + 
         COALESCE(contributors.cnt, 0) * 2) AS score,
        COALESCE(stars.cnt, 0) AS stars_7d,
        COALESCE(forks.cnt, 0) AS forks_7d,
        COALESCE(prs.cnt, 0) AS prs_7d,
        COALESCE(contributors.cnt, 0) AS contributors_7d,
        0 AS rank_position
    FROM repos r
    LEFT JOIN (
        SELECT repo_id, COUNT(*) AS cnt
        FROM events
        WHERE event_type = 'WatchEvent' AND created_at_date >= v_start_date
        GROUP BY repo_id
    ) stars ON r.repo_id = stars.repo_id
    LEFT JOIN (
        SELECT repo_id, COUNT(*) AS cnt
        FROM events
        WHERE event_type = 'ForkEvent' AND created_at_date >= v_start_date
        GROUP BY repo_id
    ) forks ON r.repo_id = forks.repo_id
    LEFT JOIN (
        SELECT repo_id, COUNT(*) AS cnt
        FROM events
        WHERE event_type = 'PullRequestEvent' AND created_at_date >= v_start_date
        GROUP BY repo_id
    ) prs ON r.repo_id = prs.repo_id
    LEFT JOIN (
        SELECT repo_id, COUNT(DISTINCT actor_id) AS cnt
        FROM events
        WHERE created_at_date >= v_start_date
        GROUP BY repo_id
    ) contributors ON r.repo_id = contributors.repo_id
    HAVING score > 0
    ORDER BY score DESC
    LIMIT 1000;
    
    -- 更新排名
    SET @rank = 0;
    UPDATE hot_repos 
    SET rank_position = (@rank := @rank + 1)
    ORDER BY score DESC;
    
    SELECT CONCAT('热门仓库榜单更新完成, ',
                  '共', ROW_COUNT(), '个仓库, ',
                  '耗时', TIMESTAMPDIFF(SECOND, v_start_time, NOW()), '秒') 
           AS message;
END //

-- 存储过程3：更新活跃开发者榜单
CREATE PROCEDURE sp_update_active_developers()
BEGIN
    DECLARE v_start_date DATE DEFAULT DATE_SUB(CURDATE(), INTERVAL 7 DAY);
    DECLARE v_start_time DATETIME DEFAULT NOW();
    
    -- 清空旧数据
    TRUNCATE TABLE active_developers;
    
    -- 生成新榜单
    INSERT INTO active_developers (
        actor_id,
        actor_login,
        activity_score,
        commits_7d,
        prs_7d,
        issues_7d,
        repos_7d,
        rank_position
    )
    SELECT 
        a.actor_id,
        a.login AS actor_login,
        -- 计算活跃分数：提交*2 + PR*3 + Issue*1 + 仓库数*1.5
        (COALESCE(commits.cnt, 0) * 2 +
         COALESCE(prs.cnt, 0) * 3 +
         COALESCE(issues.cnt, 0) * 1 +
         COALESCE(repos.cnt, 0) * 1.5) AS activity_score,
        COALESCE(commits.cnt, 0) AS commits_7d,
        COALESCE(prs.cnt, 0) AS prs_7d,
        COALESCE(issues.cnt, 0) AS issues_7d,
        COALESCE(repos.cnt, 0) AS repos_7d,
        0 AS rank_position
    FROM actors a
    LEFT JOIN (
        SELECT actor_id, COUNT(*) AS cnt
        FROM events
        WHERE event_type = 'PushEvent' AND created_at_date >= v_start_date
        GROUP BY actor_id
    ) commits ON a.actor_id = commits.actor_id
    LEFT JOIN (
        SELECT actor_id, COUNT(*) AS cnt
        FROM events
        WHERE event_type = 'PullRequestEvent' AND created_at_date >= v_start_date
        GROUP BY actor_id
    ) prs ON a.actor_id = prs.actor_id
    LEFT JOIN (
        SELECT actor_id, COUNT(*) AS cnt
        FROM events
        WHERE event_type = 'IssuesEvent' AND created_at_date >= v_start_date
        GROUP BY actor_id
    ) issues ON a.actor_id = issues.actor_id
    LEFT JOIN (
        SELECT actor_id, COUNT(DISTINCT repo_id) AS cnt
        FROM events
        WHERE created_at_date >= v_start_date
        GROUP BY actor_id
    ) repos ON a.actor_id = repos.actor_id
    HAVING activity_score > 0
    ORDER BY activity_score DESC
    LIMIT 1000;
    
    -- 更新排名
    SET @rank = 0;
    UPDATE active_developers
    SET rank_position = (@rank := @rank + 1)
    ORDER BY activity_score DESC;
    
    SELECT CONCAT('活跃开发者榜单更新完成, ',
                  '共', ROW_COUNT(), '个开发者, ',
                  '耗时', TIMESTAMPDIFF(SECOND, v_start_time, NOW()), '秒') 
           AS message;
END //

-- 存储过程4：数据清理（删除N天前的数据）
CREATE PROCEDURE sp_cleanup_old_data(
    IN p_days_to_keep INT
)
BEGIN
    DECLARE v_cutoff_date DATE;
    DECLARE v_deleted_events INT DEFAULT 0;
    DECLARE v_deleted_stats INT DEFAULT 0;
    DECLARE v_deleted_relations INT DEFAULT 0;
    
    SET v_cutoff_date = DATE_SUB(CURDATE(), INTERVAL p_days_to_keep DAY);
    
    -- 删除旧事件
    DELETE FROM events WHERE created_at_date < v_cutoff_date;
    SET v_deleted_events = ROW_COUNT();
    
    -- 删除旧统计
    DELETE FROM event_stats_daily WHERE stats_date < v_cutoff_date;
    SET v_deleted_stats = ROW_COUNT();
    
    -- 清理孤立的关联数据
    DELETE FROM user_repo_relation WHERE last_event_at < v_cutoff_date;
    SET v_deleted_relations = ROW_COUNT();
    
    SELECT CONCAT('数据清理完成（保留', p_days_to_keep, '天）: ',
                  '事件', v_deleted_events, '条, ',
                  '统计', v_deleted_stats, '条, ',
                  '关联', v_deleted_relations, '条') 
           AS message;
END //

-- 存储过程5：刷新仓库统计缓存
CREATE PROCEDURE sp_refresh_repo_stats(
    IN p_repo_id INT UNSIGNED
)
BEGIN
    DECLARE v_start_date_7d DATE DEFAULT DATE_SUB(CURDATE(), INTERVAL 7 DAY);
    DECLARE v_start_date_1d DATE DEFAULT DATE_SUB(CURDATE(), INTERVAL 1 DAY);
    DECLARE v_start_date_30d DATE DEFAULT DATE_SUB(CURDATE(), INTERVAL 30 DAY);
    
    INSERT INTO repo_stats_cache (
        repo_id,
        total_stars,
        total_forks,
        total_watchers,
        total_contributors,
        total_commits,
        total_prs,
        total_issues,
        stars_1d,
        stars_7d,
        stars_30d
    )
    SELECT 
        p_repo_id,
        COALESCE((SELECT COUNT(*) FROM events 
                  WHERE repo_id = p_repo_id AND event_type = 'WatchEvent'), 0),
        COALESCE((SELECT COUNT(*) FROM events 
                  WHERE repo_id = p_repo_id AND event_type = 'ForkEvent'), 0),
        COALESCE((SELECT COUNT(DISTINCT actor_id) FROM events 
                  WHERE repo_id = p_repo_id AND event_type = 'WatchEvent'), 0),
        COALESCE((SELECT COUNT(DISTINCT actor_id) FROM events 
                  WHERE repo_id = p_repo_id), 0),
        COALESCE((SELECT COUNT(*) FROM events 
                  WHERE repo_id = p_repo_id AND event_type = 'PushEvent'), 0),
        COALESCE((SELECT COUNT(*) FROM events 
                  WHERE repo_id = p_repo_id AND event_type = 'PullRequestEvent'), 0),
        COALESCE((SELECT COUNT(*) FROM events 
                  WHERE repo_id = p_repo_id AND event_type = 'IssuesEvent'), 0),
        COALESCE((SELECT COUNT(*) FROM events 
                  WHERE repo_id = p_repo_id AND event_type = 'WatchEvent'
                  AND created_at_date >= v_start_date_1d), 0),
        COALESCE((SELECT COUNT(*) FROM events 
                  WHERE repo_id = p_repo_id AND event_type = 'WatchEvent'
                  AND created_at_date >= v_start_date_7d), 0),
        COALESCE((SELECT COUNT(*) FROM events 
                  WHERE repo_id = p_repo_id AND event_type = 'WatchEvent'
                  AND created_at_date >= v_start_date_30d), 0)
    ON DUPLICATE KEY UPDATE
        total_stars = VALUES(total_stars),
        total_forks = VALUES(total_forks),
        total_watchers = VALUES(total_watchers),
        total_contributors = VALUES(total_contributors),
        total_commits = VALUES(total_commits),
        total_prs = VALUES(total_prs),
        total_issues = VALUES(total_issues),
        stars_1d = VALUES(stars_1d),
        stars_7d = VALUES(stars_7d),
        stars_30d = VALUES(stars_30d);
    
    SELECT CONCAT('仓库统计缓存已刷新: repo_id=', p_repo_id) AS message;
END //

DELIMITER ;

-- ========================================
-- 第八部分：触发器（补偿外键约束）
-- ========================================

DELIMITER //

-- 触发器1：插入事件前验证数据完整性
CREATE TRIGGER trg_validate_event_insert
BEFORE INSERT ON events
FOR EACH ROW
BEGIN
    DECLARE v_actor_exists INT DEFAULT 0;
    DECLARE v_repo_exists INT DEFAULT 0;
    DECLARE v_org_exists INT DEFAULT 0;
    
    -- 验证actor_id
    SELECT COUNT(*) INTO v_actor_exists FROM actors WHERE actor_id = NEW.actor_id;
    IF v_actor_exists = 0 THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Invalid actor_id: 用户不存在';
    END IF;
    
    -- 验证repo_id
    SELECT COUNT(*) INTO v_repo_exists FROM repos WHERE repo_id = NEW.repo_id;
    IF v_repo_exists = 0 THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Invalid repo_id: 仓库不存在';
    END IF;
    
    -- 验证org_id（可为空）
    IF NEW.org_id IS NOT NULL THEN
        SELECT COUNT(*) INTO v_org_exists FROM organizations WHERE org_id = NEW.org_id;
        IF v_org_exists = 0 THEN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Invalid org_id: 组织不存在';
        END IF;
    END IF;
END //

-- 触发器2：插入事件后更新用户和仓库统计
CREATE TRIGGER trg_after_event_insert
AFTER INSERT ON events
FOR EACH ROW
BEGIN
    -- 更新用户统计
    UPDATE actors 
    SET last_active_at = NEW.created_at,
        total_events = total_events + 1
    WHERE actor_id = NEW.actor_id;
    
    -- 更新仓库统计
    UPDATE repos
    SET last_event_at = NEW.created_at,
        total_events = total_events + 1
    WHERE repo_id = NEW.repo_id;
    
    -- 处理Star事件
    IF NEW.event_type = 'WatchEvent' THEN
        UPDATE repos 
        SET total_stars = total_stars + 1 
        WHERE repo_id = NEW.repo_id;
    END IF;
    
    -- 处理Fork事件
    IF NEW.event_type = 'ForkEvent' THEN
        UPDATE repos 
        SET total_forks = total_forks + 1 
        WHERE repo_id = NEW.repo_id;
    END IF;
END //

-- 触发器3：更新用户仓库关联
CREATE TRIGGER trg_update_user_repo_relation
AFTER INSERT ON events
FOR EACH ROW
BEGIN
    DECLARE v_relation_type VARCHAR(50);
    
    -- 确定关联类型
    SET v_relation_type = CASE 
        WHEN NEW.event_type = 'WatchEvent' THEN 'star'
        WHEN NEW.event_type = 'ForkEvent' THEN 'fork'
        ELSE 'contributor'
    END;
    
    -- 插入或更新关联关系
    INSERT INTO user_repo_relation (
        actor_id, 
        repo_id, 
        relation_type, 
        relation_time,
        first_event_at,
        last_event_at,
        event_count
    )
    VALUES (
        NEW.actor_id,
        NEW.repo_id,
        v_relation_type,
        NEW.created_at,
        NEW.created_at,
        NEW.created_at,
        1
    )
    ON DUPLICATE KEY UPDATE
        last_event_at = NEW.created_at,
        event_count = event_count + 1;
END //

DELIMITER ;

-- ========================================
-- 第九部分：视图
-- ========================================

-- 视图1：最近7天事件摘要
CREATE OR REPLACE VIEW v_recent_events_summary AS
SELECT 
    e.event_id,
    e.gh_event_id,
    e.event_type,
    e.created_at,
    e.actor_login,
    e.repo_name,
    a.display_login,
    r.total_stars,
    r.total_forks
FROM events e
INNER JOIN actors a ON e.actor_id = a.actor_id
INNER JOIN repos r ON e.repo_id = r.repo_id
WHERE e.created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
ORDER BY e.created_at DESC
LIMIT 10000;

-- 视图2：热门仓库排行榜
CREATE OR REPLACE VIEW v_trending_repos AS
SELECT 
    hr.repo_id,
    hr.repo_name,
    r.total_stars,
    r.total_forks,
    r.total_contributors,
    hr.score,
    hr.stars_7d,
    hr.forks_7d,
    hr.prs_7d,
    hr.contributors_7d,
    hr.rank_position
FROM hot_repos hr
INNER JOIN repos r ON hr.repo_id = r.repo_id
ORDER BY hr.rank_position
LIMIT 100;

-- 视图3：活跃开发者排行榜
CREATE OR REPLACE VIEW v_active_developers_leaderboard AS
SELECT 
    ad.actor_id,
    ad.actor_login,
    ad.activity_score,
    ad.commits_7d,
    ad.prs_7d,
    ad.issues_7d,
    ad.repos_7d,
    ad.rank_position,
    a.total_events,
    a.total_repos,
    a.last_active_at
FROM active_developers ad
INNER JOIN actors a ON ad.actor_id = a.actor_id
ORDER BY ad.rank_position
LIMIT 100;

-- 视图4：仓库详细统计
CREATE OR REPLACE VIEW v_repo_detail_stats AS
SELECT 
    r.repo_id,
    r.name,
    r.url,
    r.language,
    r.total_stars,
    r.total_forks,
    r.total_events,
    rs.total_watchers,
    rs.total_contributors,
    rs.total_commits,
    rs.total_prs,
    rs.total_issues,
    rs.stars_7d,
    rs.stars_30d,
    r.last_event_at
FROM repos r
LEFT JOIN repo_stats_cache rs ON r.repo_id = rs.repo_id;

-- 视图5：用户详细统计
CREATE OR REPLACE VIEW v_actor_detail_stats AS
SELECT 
    a.actor_id,
    a.login,
    a.display_login,
    a.avatar_url,
    a.total_events,
    a.total_repos,
    a.last_active_at,
    acs.total_commits,
    acs.total_prs,
    acs.total_issues,
    acs.total_stars_received,
    acs.commits_7d,
    acs.prs_7d
FROM actors a
LEFT JOIN actor_stats_cache acs ON a.actor_id = acs.actor_id;

-- 视图6：每日事件趋势
CREATE OR REPLACE VIEW v_daily_event_trends AS
SELECT 
    stats_date,
    event_type,
    total_count,
    unique_actors,
    unique_repos,
    ROUND(total_count / NULLIF(unique_actors, 0), 2) AS avg_events_per_actor,
    ROUND(total_count / NULLIF(unique_repos, 0), 2) AS avg_events_per_repo
FROM event_stats_daily
WHERE stats_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
ORDER BY stats_date DESC, total_count DESC;
