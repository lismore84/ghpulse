#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GHPulse 数据查询Web应用
"""

from flask import Flask, jsonify, request, render_template
import pymysql
from pymysql import cursors
import os
from dotenv import load_dotenv
from datetime import datetime
import logging
import traceback

# 配置日志
logging.basicConfig(
    level=logging.DEBUG,  # 改为DEBUG模式查看详细错误
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 加载环境变量
load_dotenv()

# 创建Flask应用
app = Flask(__name__)

# 数据库配置
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 3306)),
    'user': os.getenv('WEB_DB_USER', 'web_user'),
    'password': os.getenv('WEB_DB_PASSWORD', ''),
    'database': os.getenv('DB_NAME', 'ghpulse'),
    'charset': 'utf8mb4',
    'cursorclass': cursors.DictCursor,
    'connect_timeout': 10
}

# 显示配置（隐藏密码）
logger.info("=" * 60)
logger.info("数据库配置:")
logger.info(f"  Host: {DB_CONFIG['host']}")
logger.info(f"  Port: {DB_CONFIG['port']}")
logger.info(f"  User: {DB_CONFIG['user']}")
logger.info(f"  Database: {DB_CONFIG['database']}")
logger.info("=" * 60)


def get_db_connection():
    """获取数据库连接"""
    try:
        conn = pymysql.connect(**DB_CONFIG)
        logger.debug("数据库连接成功")
        return conn
    except Exception as e:
        logger.error(f"数据库连接失败: {e}")
        logger.error(traceback.format_exc())
        raise


# ==================== 前端路由 ====================

@app.route('/')
def index():
    """主页面"""
    try:
        logger.info("访问主页")
        return render_template('index.html')
    except Exception as e:
        logger.error(f"渲染主页失败: {e}")
        logger.error(traceback.format_exc())
        return f"<h1>错误</h1><pre>{str(e)}\n\n{traceback.format_exc()}</pre>", 500


# ==================== API路由 ====================

@app.route('/api/health', methods=['GET'])
def health_check():
    """健康检查"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        conn.close()
        return jsonify({
            'status': 'ok', 
            'message': '数据库连接正常',
            'config': {
                'host': DB_CONFIG['host'],
                'port': DB_CONFIG['port'],
                'database': DB_CONFIG['database']
            }
        })
    except Exception as e:
        logger.error(f"健康检查失败: {e}")
        return jsonify({
            'status': 'error', 
            'message': str(e),
            'traceback': traceback.format_exc()
        }), 500


@app.route('/api/tables', methods=['GET'])
def get_tables():
    """获取所有表及其统计信息"""
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                TABLE_NAME as name,
                TABLE_COMMENT as comment,
                TABLE_ROWS as row_count,
                ROUND((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024, 2) as size_mb,
                ENGINE as engine,
                CREATE_TIME as created_at
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = %s
            ORDER BY TABLE_NAME
        """, (DB_CONFIG['database'],))
        
        tables = cursor.fetchall()
        
        # 转换datetime为字符串
        for table in tables:
            if table.get('created_at'):
                table['created_at'] = table['created_at'].isoformat()
        
        cursor.close()
        
        return jsonify({
            'success': True,
            'data': tables,
            'count': len(tables)
        })
    
    except Exception as e:
        logger.error(f"获取表列表失败: {e}")
        logger.error(traceback.format_exc())
        return jsonify({
            'success': False, 
            'error': str(e),
            'traceback': traceback.format_exc()
        }), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/table/<table_name>', methods=['GET'])
def get_table_data(table_name):
    """获取表数据（分页）"""
    conn = None
    try:
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 20))
        offset = (page - 1) * page_size
        
        # 验证表名（防止SQL注入）
        valid_tables = [
            'actors', 'repos', 'organizations', 'events',
            'payload_push', 'payload_issue', 'payload_pull_request',
            'payload_star', 'payload_fork', 'payload_create', 'payload_delete',
            'payload_watch',
            'hot_repos', 'active_developers', 'event_stats_daily',
            'user_repo_relation', 'repo_stats_cache', 'actor_stats_cache'
        ]
        
        if table_name not in valid_tables:
            return jsonify({'success': False, 'error': f'无效的表名: {table_name}'}), 400
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 获取总行数
        cursor.execute(f"SELECT COUNT(*) as total FROM `{table_name}`")
        total = cursor.fetchone()['total']
        
        # 获取数据
        cursor.execute(f"SELECT * FROM `{table_name}` LIMIT %s OFFSET %s", (page_size, offset))
        rows = cursor.fetchall()
        
        # 转换datetime为字符串
        for row in rows:
            for key, value in list(row.items()):
                if isinstance(value, datetime):
                    row[key] = value.isoformat()
        
        # 获取列信息
        cursor.execute(f"DESCRIBE `{table_name}`")
        columns = [
            {
                'field': col['Field'],
                'type': col['Type'],
                'key': col['Key'],
                'comment': col.get('Extra', '')
            }
            for col in cursor.fetchall()
        ]
        
        cursor.close()
        
        return jsonify({
            'success': True,
            'data': {
                'rows': rows,
                'columns': columns,
                'pagination': {
                    'page': page,
                    'page_size': page_size,
                    'total': total,
                    'total_pages': (total + page_size - 1) // page_size
                }
            }
        })
    
    except Exception as e:
        logger.error(f"获取表数据失败: {e}")
        logger.error(traceback.format_exc())
        return jsonify({
            'success': False, 
            'error': str(e),
            'traceback': traceback.format_exc()
        }), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/query', methods=['POST'])
def execute_query():
    """执行自定义SQL查询（只读）"""
    conn = None
    try:
        data = request.get_json()
        sql = data.get('sql', '').strip()
        
        if not sql:
            return jsonify({'success': False, 'error': 'SQL不能为空'}), 400
        
        # 安全检查：只允许SELECT语句
        sql_upper = sql.upper()
        if not sql_upper.startswith('SELECT'):
            return jsonify({'success': False, 'error': '只允许执行SELECT查询'}), 400
        
        # 禁止的关键字
        forbidden_keywords = ['INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'ALTER', 'TRUNCATE']
        for keyword in forbidden_keywords:
            if keyword in sql_upper:
                return jsonify({'success': False, 'error': f'禁止使用 {keyword} 语句'}), 400
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 限制返回行数
        max_rows = 1000
        if 'LIMIT' not in sql_upper:
            sql = f"{sql} LIMIT {max_rows}"
        
        # 执行查询
        start_time = datetime.now()
        cursor.execute(sql)
        rows = cursor.fetchall()
        execution_time = (datetime.now() - start_time).total_seconds()
        
        # 转换datetime
        for row in rows:
            for key, value in list(row.items()):
                if isinstance(value, datetime):
                    row[key] = value.isoformat()
        
        # 获取列名
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        
        cursor.close()
        
        return jsonify({
            'success': True,
            'data': {
                'rows': rows,
                'columns': columns,
                'count': len(rows),
                'execution_time': execution_time
            }
        })
    
    except Exception as e:
        logger.error(f"查询执行失败: {e}")
        logger.error(traceback.format_exc())
        return jsonify({
            'success': False, 
            'error': str(e),
            'traceback': traceback.format_exc()
        }), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/stats/overview', methods=['GET'])
def get_overview_stats():
    """获取总体统计"""
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        stats = {}
        
        cursor.execute("SELECT COUNT(*) as total FROM events")
        stats['total_events'] = cursor.fetchone()['total']
        
        cursor.execute("SELECT COUNT(*) as total FROM actors")
        stats['total_actors'] = cursor.fetchone()['total']
        
        cursor.execute("SELECT COUNT(*) as total FROM repos")
        stats['total_repos'] = cursor.fetchone()['total']
        
        cursor.execute("SELECT COUNT(*) as total FROM organizations")
        stats['total_orgs'] = cursor.fetchone()['total']
        
        cursor.execute("SELECT MAX(created_at) as latest FROM events")
        result = cursor.fetchone()
        latest = result['latest'] if result else None
        stats['latest_event'] = latest.isoformat() if latest else None
        
        cursor.close()
        
        return jsonify({
            'success': True,
            'data': stats
        })
    
    except Exception as e:
        logger.error(f"获取统计失败: {e}")
        logger.error(traceback.format_exc())
        return jsonify({
            'success': False, 
            'error': str(e)
        }), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/stats/event_types', methods=['GET'])
def get_event_type_stats():
    """获取事件类型分布"""
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                event_type,
                COUNT(*) as count
            FROM events
            GROUP BY event_type
            ORDER BY count DESC
            LIMIT 20
        """)
        
        results = cursor.fetchall()
        cursor.close()
        
        return jsonify({
            'success': True,
            'data': results
        })
    
    except Exception as e:
        logger.error(f"获取事件类型统计失败: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/trending/repos', methods=['GET'])
def get_trending_repos():
    """获取热门仓库（自动降级）"""
    conn = None
    try:
        limit = int(request.args.get('limit', 10))
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 先尝试从热门表获取
        cursor.execute("SHOW TABLES LIKE 'hot_repos'")
        table_exists = cursor.fetchone()
        
        repos = []
        source = 'empty'
        
        if table_exists:
            cursor.execute("""
                SELECT 
                    repo_id,
                    repo_name,
                    score,
                    stars_7d,
                    forks_7d,
                    prs_7d,
                    rank_position
                FROM hot_repos
                ORDER BY rank_position
                LIMIT %s
            """, (limit,))
            repos = cursor.fetchall()
            source = 'cached'
        
        # 如果热门表为空或不存在，使用降级查询
        if not repos:
            logger.warning("hot_repos表为空，使用降级查询")
            
            # 检查 repos 表的实际列名
            cursor.execute("SHOW COLUMNS FROM repos")
            columns = [row['Field'] for row in cursor.fetchall()]
            logger.info(f"repos表列名: {columns}")
            
            # 使用实际的列名（根据你的表结构调整）
            name_column = 'name' if 'name' in columns else 'full_name'
            stars_column = 'total_stars' if 'total_stars' in columns else 'stargazers_count'
            
            cursor.execute(f"""
                SELECT 
                    r.repo_id,
                    r.{name_column} as repo_name,
                    COALESCE(r.{stars_column}, 0) as score,
                    COUNT(CASE 
                        WHEN e.event_type = 'WatchEvent' 
                        AND e.created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY) 
                        THEN 1 
                    END) as stars_7d,
                    COUNT(CASE 
                        WHEN e.event_type = 'ForkEvent' 
                        AND e.created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY) 
                        THEN 1 
                    END) as forks_7d,
                    COUNT(CASE 
                        WHEN e.event_type = 'PullRequestEvent' 
                        AND e.created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY) 
                        THEN 1 
                    END) as prs_7d,
                    ROW_NUMBER() OVER (ORDER BY r.{stars_column} DESC) as rank_position
                FROM repos r
                LEFT JOIN events e ON r.repo_id = e.repo_id 
                    AND e.created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
                GROUP BY r.repo_id, r.{name_column}, r.{stars_column}
                ORDER BY score DESC
                LIMIT %s
            """, (limit,))
            repos = cursor.fetchall()
            source = 'realtime'
        
        cursor.close()
        
        logger.info(f"返回 {len(repos)} 个热门仓库 (来源: {source})")
        if repos and len(repos) > 0:
            logger.info(f"第一条数据: {repos[0]}")
        
        return jsonify({
            'success': True,
            'data': repos,
            'source': source
        })
    
    except Exception as e:
        logger.error(f"获取热门仓库失败: {e}")
        logger.error(traceback.format_exc())
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/trending/developers', methods=['GET'])
def get_trending_developers():
    """获取活跃开发者（自动降级）"""
    conn = None
    try:
        limit = int(request.args.get('limit', 10))
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 先尝试从活跃表获取
        cursor.execute("SHOW TABLES LIKE 'active_developers'")
        table_exists = cursor.fetchone()
        
        developers = []
        source = 'empty'
        
        if table_exists:
            cursor.execute("""
                SELECT 
                    actor_id,
                    actor_login,
                    activity_score,
                    commits_7d,
                    prs_7d,
                    issues_7d,
                    rank_position
                FROM active_developers
                ORDER BY rank_position
                LIMIT %s
            """, (limit,))
            developers = cursor.fetchall()
            source = 'cached'
        
        # 如果活跃表为空或不存在，使用降级查询
        if not developers:
            logger.warning("active_developers表为空，使用降级查询")
            
            # 检查 actors 表的实际列名
            cursor.execute("SHOW COLUMNS FROM actors")
            columns = [row['Field'] for row in cursor.fetchall()]
            logger.info(f"actors表列名: {columns}")
            
            # 使用实际的列名
            login_column = 'login' if 'login' in columns else 'username'
            events_column = 'total_events' if 'total_events' in columns else 'public_events'
            
            cursor.execute(f"""
                SELECT 
                    a.actor_id,
                    a.{login_column} as actor_login,
                    COALESCE(a.{events_column}, 0) as activity_score,
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
                    COUNT(CASE 
                        WHEN e.event_type = 'IssuesEvent' 
                        AND e.created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY) 
                        THEN 1 
                    END) as issues_7d,
                    ROW_NUMBER() OVER (ORDER BY a.{events_column} DESC) as rank_position
                FROM actors a
                LEFT JOIN events e ON a.actor_id = e.actor_id 
                    AND e.created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
                GROUP BY a.actor_id, a.{login_column}, a.{events_column}
                ORDER BY activity_score DESC
                LIMIT %s
            """, (limit,))
            developers = cursor.fetchall()
            source = 'realtime'
        
        cursor.close()
        
        logger.info(f"返回 {len(developers)} 个活跃开发者 (来源: {source})")
        if developers and len(developers) > 0:
            logger.info(f"第一条数据: {developers[0]}")
        
        return jsonify({
            'success': True,
            'data': developers,
            'source': source
        })
    
    except Exception as e:
        logger.error(f"获取活跃开发者失败: {e}")
        logger.error(traceback.format_exc())
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        if conn:
            conn.close()


# 错误处理
@app.errorhandler(404)
def not_found(e):
    return jsonify({'success': False, 'error': '页面未找到'}), 404


@app.errorhandler(500)
def internal_error(e):
    logger.error(f"内部错误: {e}")
    logger.error(traceback.format_exc())
    return jsonify({
        'success': False, 
        'error': '内部服务器错误',
        'detail': str(e),
        'traceback': traceback.format_exc()
    }), 500


if __name__ == '__main__':
    # 检查环境变量
    required_vars = ['DB_HOST', 'DB_NAME', 'WEB_DB_USER', 'WEB_DB_PASSWORD']
    missing = [v for v in required_vars if not os.getenv(v)]
    
    if missing:
        logger.error(f"缺少环境变量: {', '.join(missing)}")
        logger.error("请检查 .env 文件")
        exit(1)
    
    # 测试数据库连接
    try:
        test_conn = get_db_connection()
        test_conn.close()
        logger.info("✓ 数据库连接测试成功")
    except Exception as e:
        logger.error("✗ 数据库连接测试失败")
        logger.error(str(e))
        exit(1)
    
    logger.info("=" * 60)
    logger.info("🚀 启动 GHPulse Web 应用")
    logger.info(f"📍 访问地址: http://localhost:5000")
    logger.info(f"📍 API文档: http://localhost:5000/api/health")
    logger.info("=" * 60)
    
    app.run(
        host='0.0.0.0',
        port=5000,
        debug=True
    )