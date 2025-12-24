#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GHPulse 完整统计更新脚本
更新所有统计和缓存表，适合定时任务运行

更新的表：
1. hot_repos - 热门仓库榜单
2. active_developers - 活跃开发者榜单
3. actor_stats_cache - 用户统计缓存
4. repo_stats_cache - 仓库统计缓存
5. event_stats_daily - 每日事件统计
6. base_stats - 基础统计数据
"""

import pymysql
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import logging
import sys

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('stats_update.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'port': int(os.getenv('DB_PORT', 3306)),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME'),
    'charset': 'utf8mb4'
}


def get_db_connection():
    """获取数据库连接"""
    try:
        return pymysql.connect(**DB_CONFIG)
    except Exception as e:
        logger.error(f"数据库连接失败: {e}")
        raise


def update_hot_repos():
    """更新热门仓库榜单"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        logger.info("=" * 60)
        logger.info("🔥 更新热门仓库榜单")
        logger.info("=" * 60)
        
        # 检查表是否存在
        cursor.execute("SHOW TABLES LIKE 'hot_repos'")
        if not cursor.fetchone():
            logger.error("❌ hot_repos 表不存在，请先运行初始化脚本")
            return
        
        cursor.execute("DELETE FROM hot_repos")
        deleted = cursor.rowcount
        logger.info(f"✓ 清空旧数据: {deleted} 行")
        
        logger.info("⏳ 计算热门仓库（基于星标、Fork、PR 活跃度）...")
        cursor.execute("""
            INSERT INTO hot_repos (
                repo_id, repo_name, score, 
                stars_7d, forks_7d, prs_7d, 
                rank_position, updated_at
            )
            SELECT 
                r.repo_id,
                r.name as repo_name,
                COALESCE(r.total_stars, 0) + 
                    COUNT(CASE WHEN e.event_type = 'WatchEvent' 
                        AND e.created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY) THEN 1 END) * 2 +
                    COUNT(CASE WHEN e.event_type = 'ForkEvent' 
                        AND e.created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY) THEN 1 END) * 1.5 as score,
                COUNT(CASE WHEN e.event_type = 'WatchEvent' 
                    AND e.created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY) THEN 1 END) as stars_7d,
                COUNT(CASE WHEN e.event_type = 'ForkEvent' 
                    AND e.created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY) THEN 1 END) as forks_7d,
                COUNT(CASE WHEN e.event_type = 'PullRequestEvent' 
                    AND e.created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY) THEN 1 END) as prs_7d,
                ROW_NUMBER() OVER (ORDER BY 
                    COALESCE(r.total_stars, 0) + 
                    COUNT(CASE WHEN e.event_type = 'WatchEvent' 
                        AND e.created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY) THEN 1 END) * 2 +
                    COUNT(CASE WHEN e.event_type = 'ForkEvent' 
                        AND e.created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY) THEN 1 END) * 1.5
                    DESC
                ) as rank_position,
                NOW() as updated_at
            FROM repos r
            LEFT JOIN events e ON r.repo_id = e.repo_id
            GROUP BY r.repo_id, r.name, r.total_stars
            HAVING score > 0
            ORDER BY score DESC
            LIMIT 100
        """)
        
        count = cursor.rowcount
        conn.commit()
        logger.info(f"✓ 成功插入 {count} 个热门仓库")
        
        # 显示 Top 3
        cursor.execute("""
            SELECT rank_position, repo_name, score, stars_7d, forks_7d, prs_7d
            FROM hot_repos ORDER BY rank_position LIMIT 3
        """)
        logger.info("\n📊 Top 3 热门仓库:")
        for row in cursor.fetchall():
            logger.info(f"  #{row[0]} {row[1]} - 得分:{row[2]:.1f} ⭐7日:{row[3]} 🍴7日:{row[4]}")
        
    except Exception as e:
        logger.error(f"❌ 更新失败: {e}")
        import traceback
        logger.error(traceback.format_exc())
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


def update_active_developers():
    """更新活跃开发者榜单"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        logger.info("=" * 60)
        logger.info("👨‍💻 更新活跃开发者榜单")
        logger.info("=" * 60)
        
        cursor.execute("SHOW TABLES LIKE 'active_developers'")
        if not cursor.fetchone():
            logger.error("❌ active_developers 表不存在")
            return
        
        cursor.execute("DELETE FROM active_developers")
        deleted = cursor.rowcount
        logger.info(f"✓ 清空旧数据: {deleted} 行")
        
        logger.info("⏳ 计算活跃开发者（基于提交、PR、Issue 活跃度）...")
        cursor.execute("""
            INSERT INTO active_developers (
                actor_id, actor_login, activity_score,
                commits_7d, prs_7d, issues_7d, repos_7d,
                rank_position, updated_at
            )
            SELECT 
                a.actor_id,
                a.login as actor_login,
                COALESCE(a.total_events, 0) + 
                    COUNT(CASE WHEN e.created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY) THEN 1 END) as activity_score,
                COUNT(CASE WHEN e.event_type = 'PushEvent' 
                    AND e.created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY) THEN 1 END) as commits_7d,
                COUNT(CASE WHEN e.event_type = 'PullRequestEvent' 
                    AND e.created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY) THEN 1 END) as prs_7d,
                COUNT(CASE WHEN e.event_type = 'IssuesEvent' 
                    AND e.created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY) THEN 1 END) as issues_7d,
                COUNT(DISTINCT CASE WHEN e.created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY) THEN e.repo_id END) as repos_7d,
                ROW_NUMBER() OVER (ORDER BY 
                    COALESCE(a.total_events, 0) + 
                    COUNT(CASE WHEN e.created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY) THEN 1 END) 
                    DESC
                ) as rank_position,
                NOW() as updated_at
            FROM actors a
            LEFT JOIN events e ON a.actor_id = e.actor_id
            GROUP BY a.actor_id, a.login, a.total_events
            HAVING activity_score > 0
            ORDER BY activity_score DESC
            LIMIT 100
        """)
        
        count = cursor.rowcount
        conn.commit()
        logger.info(f"✓ 成功插入 {count} 个活跃开发者")
        
        # 显示 Top 3
        cursor.execute("""
            SELECT rank_position, actor_login, activity_score, commits_7d, prs_7d, repos_7d
            FROM active_developers ORDER BY rank_position LIMIT 3
        """)
        logger.info("\n📊 Top 3 活跃开发者:")
        for row in cursor.fetchall():
            logger.info(f"  #{row[0]} {row[1]} - 得分:{row[2]} 📝7日提交:{row[3]} 🔀PR:{row[4]} 📦仓库:{row[5]}")
        
    except Exception as e:
        logger.error(f"❌ 更新失败: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


def update_actor_stats_cache():
    """更新用户统计缓存（全量）"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        logger.info("=" * 60)
        logger.info("📈 更新用户统计缓存")
        logger.info("=" * 60)
        
        cursor.execute("SHOW TABLES LIKE 'actor_stats_cache'")
        if not cursor.fetchone():
            logger.error("❌ actor_stats_cache 表不存在")
            return
        
        cursor.execute("DELETE FROM actor_stats_cache")
        deleted = cursor.rowcount
        logger.info(f"✓ 清空旧数据: {deleted} 行")
        
        logger.info("⏳ 计算用户统计（可能需要几分钟）...")
        cursor.execute("""
            INSERT INTO actor_stats_cache (
                actor_id,
                total_commits,
                total_prs,
                total_issues,
                total_repos,
                total_stars_received,
                commits_7d,
                prs_7d,
                repos_7d,
                updated_at
            )
            SELECT 
                a.actor_id,
                COUNT(CASE WHEN e.event_type = 'PushEvent' THEN 1 END) as total_commits,
                COUNT(CASE WHEN e.event_type = 'PullRequestEvent' THEN 1 END) as total_prs,
                COUNT(CASE WHEN e.event_type = 'IssuesEvent' THEN 1 END) as total_issues,
                COUNT(DISTINCT e.repo_id) as total_repos,
                COUNT(CASE WHEN e.event_type = 'WatchEvent' THEN 1 END) as total_stars_received,
                COUNT(CASE 
                    WHEN e.event_type = 'PushEvent' 
                    AND e.created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY) 
                    THEN 1 
                END) as commits_7d,
                COUNT(CASE 
                    WHEN e.event_type = 'PullRequestEvent' 
                    AND e.created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY) 
                    THEN 1 
                END) as prs_7d,
                COUNT(DISTINCT CASE 
                    WHEN e.created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY) 
                    THEN e.repo_id 
                END) as repos_7d,
                NOW() as updated_at
            FROM actors a
            LEFT JOIN events e ON a.actor_id = e.actor_id
            GROUP BY a.actor_id
            HAVING total_commits > 0 OR total_prs > 0 OR total_issues > 0
        """)
        
        count = cursor.rowcount
        conn.commit()
        logger.info(f"✓ 成功插入 {count} 个用户统计")
        
    except Exception as e:
        logger.error(f"❌ 更新失败: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


def update_repo_stats_cache():
    """更新仓库统计缓存（全量）"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        logger.info("=" * 60)
        logger.info("📦 更新仓库统计缓存")
        logger.info("=" * 60)
        
        # 检查表是否存在
        cursor.execute("SHOW TABLES LIKE 'repo_stats_cache'")
        if not cursor.fetchone():
            logger.warning("⚠️  repo_stats_cache 表不存在，跳过")
            return
        
        cursor.execute("DELETE FROM repo_stats_cache")
        deleted = cursor.rowcount
        logger.info(f"✓ 清空旧数据: {deleted} 行")
        
        logger.info("⏳ 计算仓库统计（可能需要几分钟）...")
        
        # 根据实际表结构，从 repos 和 events 聚合数据
        cursor.execute("""
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
                stars_30d,
                updated_at
            )
            SELECT 
                r.repo_id,
                
                -- 历史累计数据（从 repos 表直接读取）
                COALESCE(r.total_stars, 0) as total_stars,
                COALESCE(r.total_forks, 0) as total_forks,
                
                -- total_watchers 从 events 计算（WatchEvent 的唯一用户数）
                COUNT(DISTINCT CASE WHEN e.event_type = 'WatchEvent' THEN e.actor_id END) as total_watchers,
                
                -- total_contributors 从 repos 表或 events 计算
                GREATEST(
                    COALESCE(r.total_contributors, 0),
                    COUNT(DISTINCT e.actor_id)
                ) as total_contributors,
                
                -- 从 events 聚合的统计
                COUNT(CASE WHEN e.event_type = 'PushEvent' THEN 1 END) as total_commits,
                COUNT(CASE WHEN e.event_type = 'PullRequestEvent' THEN 1 END) as total_prs,
                COUNT(CASE WHEN e.event_type = 'IssuesEvent' THEN 1 END) as total_issues,
                
                -- 近期星标增量
                COUNT(CASE 
                    WHEN e.event_type = 'WatchEvent' 
                    AND e.created_at >= DATE_SUB(NOW(), INTERVAL 1 DAY) 
                    THEN 1 
                END) as stars_1d,
                COUNT(CASE 
                    WHEN e.event_type = 'WatchEvent' 
                    AND e.created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY) 
                    THEN 1 
                END) as stars_7d,
                COUNT(CASE 
                    WHEN e.event_type = 'WatchEvent' 
                    AND e.created_at >= DATE_SUB(NOW(), INTERVAL 30 DAY) 
                    THEN 1 
                END) as stars_30d,
                
                NOW() as updated_at
                
            FROM repos r
            LEFT JOIN events e ON r.repo_id = e.repo_id
            GROUP BY r.repo_id, r.total_stars, r.total_forks, r.total_contributors
            HAVING total_commits > 0 OR total_prs > 0 OR total_issues > 0 OR stars_7d > 0
        """)
        
        count = cursor.rowcount
        conn.commit()
        logger.info(f"✓ 成功插入 {count} 个仓库统计")
        
        # 显示统计摘要
        cursor.execute("""
            SELECT 
                COUNT(*) as total_repos,
                SUM(total_stars) as sum_stars,
                SUM(total_watchers) as sum_watchers,
                SUM(stars_7d) as sum_stars_7d,
                MAX(total_stars) as max_stars
            FROM repo_stats_cache
        """)
        row = cursor.fetchone()
        if row and row[0] > 0:
            logger.info(f"\n📊 仓库统计摘要:")
            logger.info(f"  总仓库数: {row[0]:,}")
            logger.info(f"  总星标数: {row[1]:,}")
            logger.info(f"  总关注数: {row[2]:,}")
            logger.info(f"  7日新增星标: {row[3]:,}")
            logger.info(f"  最高星标: {row[4]:,}")
        
        # 显示 Top 3 仓库
        cursor.execute("""
            SELECT repo_id, total_stars, total_watchers, stars_7d, total_commits, total_prs
            FROM repo_stats_cache
            ORDER BY total_stars DESC
            LIMIT 3
        """)
        logger.info("\n📊 Top 3 仓库（按星标）:")
        for row in cursor.fetchall():
            logger.info(f"  repo_id:{row[0]} ⭐{row[1]:,} 👀{row[2]:,} (7日+{row[3]}) 📝{row[4]:,}提交 🔀{row[5]:,}PR")
        
    except Exception as e:
        logger.error(f"❌ 更新失败: {e}")
        import traceback
        logger.error(traceback.format_exc())
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


def update_base_statistics():
    """更新基础统计数据（actors、repos表的统计信息）"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        logger.info("=" * 60)
        logger.info("📊 更新基础统计数据")
        logger.info("=" * 60)
        
        # 更新actors统计
        logger.info("  更新用户统计...")
        cursor.execute("""
            UPDATE actors a
            INNER JOIN (
                SELECT 
                    actor_id,
                    MAX(created_at) AS last_active,
                    COUNT(*) AS event_count
                FROM events
                GROUP BY actor_id
            ) e ON a.actor_id = e.actor_id
            SET 
                a.last_active_at = GREATEST(COALESCE(a.last_active_at, '1970-01-01'), e.last_active),
                a.total_events = a.total_events + e.event_count
        """)
        logger.info(f"    更新了 {cursor.rowcount} 个用户")
        
        # 更新repos统计
        logger.info("  更新仓库统计...")
        cursor.execute("""
            UPDATE repos r
            INNER JOIN (
                SELECT 
                    repo_id,
                    MAX(created_at) AS last_event,
                    COUNT(*) AS event_count,
                    SUM(CASE WHEN event_type = 'WatchEvent' THEN 1 ELSE 0 END) AS stars,
                    SUM(CASE WHEN event_type = 'ForkEvent' THEN 1 ELSE 0 END) AS forks
                FROM events
                GROUP BY repo_id
            ) e ON r.repo_id = e.repo_id
            SET 
                r.last_event_at = GREATEST(COALESCE(r.last_event_at, '1970-01-01'), e.last_event),
                r.total_events = r.total_events + e.event_count,
                r.total_stars = r.total_stars + e.stars,
                r.total_forks = r.total_forks + e.forks
        """)
        logger.info(f"    更新了 {cursor.rowcount} 个仓库")
        
        # 更新用户-仓库关联
        logger.info("  更新用户-仓库关联...")
        cursor.execute("""
            INSERT INTO user_repo_relation (
                actor_id, repo_id, relation_type, relation_time,
                first_event_at, last_event_at, event_count
            )
            SELECT 
                e.actor_id,
                e.repo_id,
                CASE 
                    WHEN e.event_type = 'WatchEvent' THEN 'star'
                    WHEN e.event_type = 'ForkEvent' THEN 'fork'
                    ELSE 'contributor'
                END AS relation_type,
                MIN(e.created_at) AS relation_time,
                MIN(e.created_at) AS first_event_at,
                MAX(e.created_at) AS last_event_at,
                COUNT(*) AS event_count
            FROM events e
            INNER JOIN actors a ON e.actor_id = a.actor_id  -- 确保actor存在
            INNER JOIN repos r ON e.repo_id = r.repo_id      -- 确保repo存在
            GROUP BY e.actor_id, e.repo_id, 
                CASE 
                    WHEN e.event_type = 'WatchEvent' THEN 'star'
                    WHEN e.event_type = 'ForkEvent' THEN 'fork'
                    ELSE 'contributor'
                END
            ON DUPLICATE KEY UPDATE
                last_event_at = VALUES(last_event_at),
                event_count = event_count + VALUES(event_count)
        """)
        logger.info(f"    更新了 {cursor.rowcount} 条关联")
        
        conn.commit()
        logger.info("  ✓ 基础统计数据更新完成")
        
    except Exception as e:
        logger.error(f"  ✗ 基础统计更新失败: {e}")
        import traceback
        logger.error(traceback.format_exc())
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


def update_event_stats_daily(days=30):
    """
    更新每日事件统计
    
    Args:
        days: 更新最近几天的数据（默认30天）
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        logger.info("=" * 60)
        logger.info(f"📅 更新每日事件统计（最近 {days} 天）")
        logger.info("=" * 60)
        
        # 检查表是否存在
        cursor.execute("SHOW TABLES LIKE 'event_stats_daily'")
        if not cursor.fetchone():
            logger.error("❌ event_stats_daily 表不存在")
            return
        
        # 删除最近N天的数据，重新计算
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        cursor.execute("""
            DELETE FROM event_stats_daily 
            WHERE stats_date >= %s
        """, (start_date,))
        deleted = cursor.rowcount
        logger.info(f"✓ 清空最近 {days} 天的旧数据: {deleted} 行")
        
        logger.info("⏳ 计算每日事件统计...")
        
        # 使用正确的字段名：total_count, unique_actors, unique_repos, unique_orgs, stats_time
        cursor.execute("""
            INSERT INTO event_stats_daily (
                stats_date,
                event_type,
                total_count,
                unique_actors,
                unique_repos,
                unique_orgs,
                stats_time
            )
            SELECT 
                DATE(e.created_at) as stats_date,
                e.event_type,
                COUNT(*) as total_count,
                COUNT(DISTINCT e.actor_id) as unique_actors,
                COUNT(DISTINCT e.repo_id) as unique_repos,
                COUNT(DISTINCT e.org_id) as unique_orgs,
                NOW() as stats_time
            FROM events e
            WHERE e.created_at >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
            GROUP BY DATE(e.created_at), e.event_type
            ORDER BY stats_date DESC, total_count DESC
        """, (days,))
        
        count = cursor.rowcount
        conn.commit()
        logger.info(f"✓ 成功插入 {count} 条每日统计记录")
        
        # 显示最近3天的统计摘要（使用正确的字段名）
        cursor.execute("""
            SELECT 
                stats_date,
                SUM(total_count) as total_events,
                COUNT(DISTINCT event_type) as event_types,
                SUM(unique_actors) as total_actors,
                SUM(unique_repos) as total_repos
            FROM event_stats_daily
            WHERE stats_date >= DATE_SUB(CURDATE(), INTERVAL 3 DAY)
            GROUP BY stats_date
            ORDER BY stats_date DESC
            LIMIT 3
        """)
        
        logger.info("\n📊 最近3天事件统计摘要:")
        for row in cursor.fetchall():
            logger.info(f"  {row[0]} - 事件:{row[1]:,} | 类型:{row[2]} | 用户:{row[3]:,} | 仓库:{row[4]:,}")
        
    except Exception as e:
        logger.error(f"❌ 更新失败: {e}")
        import traceback
        logger.error(traceback.format_exc())
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


def show_summary():
    """显示所有统计表的摘要"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        logger.info("\n" + "=" * 60)
        logger.info("📊 统计表摘要")
        logger.info("=" * 60)
        
        tables = [
            ('hot_repos', '热门仓库榜单'),
            ('active_developers', '活跃开发者榜单'),
            ('actor_stats_cache', '用户统计缓存'),
            ('repo_stats_cache', '仓库统计缓存'),
            ('event_stats_daily', '每日事件统计')
        ]
        
        for table, name in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                
                # 获取最后更新时间
                try:
                    # event_stats_daily 表用 stats_time，其他表用 updated_at
                    time_column = 'stats_time' if table == 'event_stats_daily' else 'updated_at'
                    cursor.execute(f"""
                        SELECT MAX({time_column}) FROM {table} 
                        WHERE {time_column} IS NOT NULL
                    """)
                    last_update = cursor.fetchone()[0]
                    if last_update:
                        time_str = last_update.strftime('%Y-%m-%d %H:%M:%S')
                    else:
                        time_str = '未知'
                except:
                    time_str = 'N/A'
                
                logger.info(f"✓ {name:20s}: {count:8,d} 条 | 最后更新: {time_str}")
            except Exception as e:
                logger.warning(f"⚠️  {name:20s}: 查询失败 ({e})")
        
    except Exception as e:
        logger.error(f"显示摘要失败: {e}")
    finally:
        cursor.close()
        conn.close()


def main():
    """主函数 - 执行所有统计更新"""
    
    start_time = datetime.now()
    
    logger.info("\n" + " 🚀 " + "=" * 58)
    logger.info(" 🚀 GHPulse 统计更新任务开始")
    logger.info(" 🚀 " + "=" * 58)
    logger.info(f"开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("更新范围: 所有统计数据")
    logger.info("")
    
    # 无条件更新所有统计数据
    update_hot_repos()
    update_active_developers()
    update_actor_stats_cache()
    update_repo_stats_cache()
    update_event_stats_daily(30)  # 默认更新30天的每日统计
    update_base_statistics()  # 更新基础统计数据
    
    # 显示摘要
    show_summary()
    
    elapsed = (datetime.now() - start_time).total_seconds()
    
    logger.info("\n" + "=" * 60)
    logger.info(f"✓ 统计更新完成！耗时: {elapsed:.2f} 秒")
    logger.info("=" * 60)
    logger.info("\n💡 提示:")
    logger.info("  - 可设置定时任务每小时运行: 0 * * * * python update_all_stats.py")
    logger.info("=" * 60)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("\n⚠️  用户中断执行")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n❌ 执行失败: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)