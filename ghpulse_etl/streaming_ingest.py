"""
GH Archive数据摄取脚本
admin_user: 管理触发器
ingest_user: 插入数据
"""

import os
import gzip
import json
import logging
from datetime import datetime
from typing import Dict, Any, List, Set
from io import BytesIO
from dotenv import load_dotenv
import pymysql
from pymysql import cursors
import requests
import argparse

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('gh_archive_ingest.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class DualConnectionIngestor:
    """双连接GH Archive数据摄取器（权限分离）"""
    
    GH_ARCHIVE_URL = "https://data.gharchive.org/{year}-{month:02d}-{day:02d}-{hour}.json.gz"
    
    def __init__(self):
        load_dotenv()
        # 数据写入连接配置（ingest_user）
        self.ingest_config = {
            'host': os.getenv('DB_HOST'),
            'port': int(os.getenv('DB_PORT', 3306)),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'database': os.getenv('DB_NAME'),
            'charset': 'utf8mb4',
            'cursorclass': cursors.DictCursor,
            'autocommit': False,
            'connect_timeout': 30
        }
        # 管理员连接配置（admin_user）
        self.admin_config = {
            'host': os.getenv('DB_HOST'),
            'port': int(os.getenv('DB_PORT', 3306)),
            'user': os.getenv('ADMIN_USER'),
            'password': os.getenv('ADMIN_PASSWORD'),
            'database': os.getenv('DB_NAME'),
            'charset': 'utf8mb4',
            'cursorclass': cursors.DictCursor,
            'autocommit': False,
            'connect_timeout': 30
        }
        self._validate_config()
        
        self.existing_actors: Set[int] = set()
        self.existing_repos: Set[int] = set()
        self.existing_orgs: Set[int] = set()
        
        self.stats = {
            'events_inserted': 0,
            'actors_inserted': 0,
            'repos_inserted': 0,
            'skipped': 0
        }
    
    def _validate_config(self):
        """验证环境变量"""
        required = ['DB_HOST', 'DB_USER', 'DB_PASSWORD', 'DB_NAME', 'ADMIN_USER', 'ADMIN_PASSWORD']
        missing = [var for var in required if not os.getenv(var)]
        if missing:
            raise ValueError(f"缺少环境变量: {', '.join(missing)}")
        logger.info(f"✓ 配置验证通过")
    
    def get_ingest_connection(self):
        """获取数据写入连接（ingest_user）"""
        try:
            conn = pymysql.connect(**self.ingest_config)
            conn.autocommit(False)
            logger.info("✓ 数据写入连接成功（ingest_user）")
            return conn
        except Exception as e:
            logger.error(f"✗ 数据写入连接失败: {e}")
            raise
    
    def get_admin_connection(self):
        """获取管理员连接（admin_user）"""
        try:
            conn = pymysql.connect(**self.admin_config)
            conn.autocommit(False)
            logger.info("✓ 管理员连接成功（admin_user）")
            return conn
        except Exception as e:
            logger.error(f"✗ 管理员连接失败: {e}")
            raise
    
    def disable_triggers(self):
        """禁用触发器（使用admin连接）"""
        admin_conn = None
        try:
            admin_conn = self.get_admin_connection()
            cursor = admin_conn.cursor()
            logger.info("正在禁用触发器...")
            
            trigger_names = [
                'trg_validate_event_insert',
                'trg_after_event_insert',
                'trg_update_user_repo_relation'
            ]
            
            for trigger_name in trigger_names:
                try:
                    cursor.execute(f"DROP TRIGGER IF EXISTS {trigger_name}")
                    logger.info(f"  ✓ 已删除触发器: {trigger_name}")
                except Exception as e:
                    logger.warning(f"  ⚠ 删除触发器失败 {trigger_name}: {e}")
            
            admin_conn.commit()
            logger.info("✓ 触发器已禁用")
            
        except Exception as e:
            logger.error(f"✗ 禁用触发器失败: {e}")
            raise
        finally:
            if admin_conn:
                admin_conn.close()
    
    def enable_triggers(self):
        """重新启用触发器（使用admin连接）"""
        admin_conn = None
        try:
            admin_conn = self.get_admin_connection()
            cursor = admin_conn.cursor()
            logger.info("正在重新启用触发器...")
            
            # 重新创建验证触发器
            validate_trigger_sql = """
                CREATE TRIGGER trg_validate_event_insert
                BEFORE INSERT ON events
                FOR EACH ROW
                BEGIN
                    DECLARE v_actor_exists INT DEFAULT 0;
                    DECLARE v_repo_exists INT DEFAULT 0;
                    
                    SELECT COUNT(*) INTO v_actor_exists FROM actors WHERE actor_id = NEW.actor_id;
                    IF v_actor_exists = 0 THEN
                        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Invalid actor_id';
                    END IF;
                    
                    SELECT COUNT(*) INTO v_repo_exists FROM repos WHERE repo_id = NEW.repo_id;
                    IF v_repo_exists = 0 THEN
                        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Invalid repo_id';
                    END IF;
                END
            """
            
            # 重新创建事件后统计更新触发器
            after_insert_trigger_sql = """
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
                END
            """
            
            # 重新创建用户仓库关联更新触发器
            relation_trigger_sql = """
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
                END
            """
            
            # 删除所有触发器
            cursor.execute("DROP TRIGGER IF EXISTS trg_validate_event_insert")
            cursor.execute("DROP TRIGGER IF EXISTS trg_after_event_insert")
            cursor.execute("DROP TRIGGER IF EXISTS trg_update_user_repo_relation")
            
            # 创建所有触发器
            cursor.execute(validate_trigger_sql)
            cursor.execute(after_insert_trigger_sql)
            cursor.execute(relation_trigger_sql)
            
            admin_conn.commit()
            logger.info("  ✓ 所有触发器已重新启用")
            
        except Exception as e:
            logger.warning(f"  ⚠ 重新启用触发器失败: {e}")
        finally:
            if admin_conn:
                admin_conn.close()
    

    
    def load_existing_ids(self, conn):
        """预加载已存在的ID"""
        cursor = conn.cursor()
        logger.info("正在加载已存在的ID...")
        
        cursor.execute("SELECT actor_id FROM actors")
        self.existing_actors = {row['actor_id'] for row in cursor.fetchall()}
        
        cursor.execute("SELECT repo_id FROM repos")
        self.existing_repos = {row['repo_id'] for row in cursor.fetchall()}
        
        cursor.execute("SELECT org_id FROM organizations")
        self.existing_orgs = {row['org_id'] for row in cursor.fetchall()}
        
        logger.info(f"  已有: {len(self.existing_actors)}用户, {len(self.existing_repos)}仓库, {len(self.existing_orgs)}组织")
        cursor.close()
    
    def stream_download_and_process(self, year: int, month: int, day: int, hour: int):
        """流式下载并处理"""
        url = self.GH_ARCHIVE_URL.format(year=year, month=month, day=day, hour=hour)
        target_date = f"{year}-{month:02d}-{day:02d}"
        logger.info(f"开始处理: {target_date} {hour:02d}:00")
        
        ingest_conn = None
        admin_conn = None
        
        try:
            # 步骤1: 使用admin禁用触发器
            self.disable_triggers()
            
            # 步骤2: 使用ingest连接处理数据
            ingest_conn = self.get_ingest_connection()
            
            # 禁用外键检查
            cursor = ingest_conn.cursor()
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
            cursor.close()
            
            self.load_existing_ids(ingest_conn)
            
            # 步骤3: 下载数据
            logger.info("正在下载...")
            response = requests.get(url, stream=True, timeout=300)
            response.raise_for_status()
            
            events = []
            logger.info("正在解压...")
            with gzip.GzipFile(fileobj=BytesIO(response.content)) as gz_file:
                for line in gz_file:
                    try:
                        event = json.loads(line.decode('utf-8'))
                        events.append(event)
                    except:
                        pass
            
            logger.info(f"✓ 下载完成，共 {len(events)} 条事件")
            
            # 步骤4: 批量处理
            self._process_all_events(ingest_conn, events)
            
            # 步骤5: 恢复外键检查
            cursor = ingest_conn.cursor()
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
            cursor.close()
            
            logger.info(f"✓ 数据插入完成")
            self._print_stats()
            
        except Exception as e:
            logger.error(f"✗ 处理失败: {e}")
            if ingest_conn:
                ingest_conn.rollback()
            raise
        finally:
            if ingest_conn:
                ingest_conn.close()
            
            # 步骤6: 使用admin重新启用触发器
            try:
                self.enable_triggers()
            except Exception as e:
                logger.error(f"重新启用触发器失败: {e}")
    
    def _process_all_events(self, conn, events: List[Dict]):
        """批量处理所有事件"""
        cursor = conn.cursor()
        
        try:
            # 收集实体
            logger.info("  [1/4] 收集实体数据...")
            actors_to_insert = []
            repos_to_insert = []
            orgs_to_insert = []
            
            for event in events:
                if 'actor' in event and event['actor'] and event['actor'].get('id'):
                    actor_id = event['actor']['id']
                    if actor_id not in self.existing_actors:
                        actors_to_insert.append(event['actor'])
                        self.existing_actors.add(actor_id)
                
                if 'repo' in event and event['repo'] and event['repo'].get('id'):
                    repo_id = event['repo']['id']
                    if repo_id not in self.existing_repos:
                        repos_to_insert.append(event['repo'])
                        self.existing_repos.add(repo_id)
                
                if 'org' in event and event['org'] and event['org'].get('id'):
                    org_id = event['org']['id']
                    if org_id not in self.existing_orgs:
                        orgs_to_insert.append(event['org'])
                        self.existing_orgs.add(org_id)
            
            # 批量插入实体
            logger.info("  [2/4] 批量插入实体...")
            if actors_to_insert:
                self._bulk_insert_actors(cursor, actors_to_insert)
            if repos_to_insert:
                self._bulk_insert_repos(cursor, repos_to_insert)
            if orgs_to_insert:
                self._bulk_insert_orgs(cursor, orgs_to_insert)
            
            conn.commit()
            logger.info("    ✓ 实体插入完成")
            
            # 批量插入Payload
            logger.info("  [3/4] 批量插入Payload...")
            payload_id_map = self._bulk_insert_payloads(cursor, events)
            conn.commit()
            logger.info("    ✓ Payload插入完成")
            
            # 批量插入Events
            logger.info("  [4/4] 批量插入事件...")
            self._bulk_insert_events_safe(cursor, events, payload_id_map)
            conn.commit()
            logger.info("    ✓ 事件插入完成")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"✗ 批量处理失败: {e}")
            raise
        finally:
            cursor.close()
    
    def _bulk_insert_actors(self, cursor, actors):
        """批量插入用户"""
        if not actors:
            return
        
        sql = "INSERT IGNORE INTO actors (actor_id, login, display_login, gravatar_id, url, avatar_url) VALUES (%s, %s, %s, %s, %s, %s)"
        values = [
            (a.get('id'), a.get('login', '')[:100], a.get('display_login', a.get('login', ''))[:100],
             a.get('gravatar_id', '')[:100], a.get('url', '')[:255], a.get('avatar_url', '')[:255])
            for a in actors if a.get('id')
        ]
        if values:
            cursor.executemany(sql, values)
            self.stats['actors_inserted'] = cursor.rowcount
            logger.info(f"    插入 {cursor.rowcount} 个新用户")
    
    def _bulk_insert_repos(self, cursor, repos):
        """批量插入仓库"""
        if not repos:
            return
        
        sql = "INSERT IGNORE INTO repos (repo_id, name, url) VALUES (%s, %s, %s)"
        values = [
            (r.get('id'), r.get('name', '')[:255], r.get('url', '')[:255])
            for r in repos if r.get('id')
        ]
        if values:
            cursor.executemany(sql, values)
            self.stats['repos_inserted'] = cursor.rowcount
            logger.info(f"    插入 {cursor.rowcount} 个新仓库")
    
    def _bulk_insert_orgs(self, cursor, orgs):
        """批量插入组织"""
        if not orgs:
            return
        
        sql = "INSERT IGNORE INTO organizations (org_id, login, gravatar_id, url, avatar_url) VALUES (%s, %s, %s, %s, %s)"
        values = [
            (o.get('id'), o.get('login', '')[:100], o.get('gravatar_id', '')[:100],
             o.get('url', '')[:255], o.get('avatar_url', '')[:255])
            for o in orgs if o.get('id')
        ]
        if values:
            cursor.executemany(sql, values)
            logger.info(f"    插入 {cursor.rowcount} 个新组织")
    
    def _bulk_insert_payloads(self, cursor, events: List[Dict]) -> Dict[int, int]:
        """批量插入Payload"""
        payload_id_map = {}
        
        push_events = []
        watch_events = []
        fork_events = []
        create_events = []
        
        for idx, event in enumerate(events):
            event_type = event.get('type', '')
            payload = event.get('payload', {})
            
            if event_type == 'PushEvent':
                push_events.append((idx, payload))
            elif event_type == 'WatchEvent':
                watch_events.append((idx, event.get('repo', {}).get('id', 0)))
            elif event_type == 'ForkEvent':
                fork_events.append((idx, payload))
            elif event_type == 'CreateEvent':
                create_events.append((idx, payload))
            else:
                payload_id_map[idx] = 1
        
        if push_events:
            sql = "INSERT INTO payload_push (push_id, size, distinct_size, head, ref) VALUES (%s, %s, %s, %s, %s)"
            values = [(p.get('push_id'), p.get('size', 0), p.get('distinct_size', 0),
                      (p.get('head') or '')[:100], (p.get('ref') or '')[:255]) for idx, p in push_events]
            cursor.executemany(sql, values)
            start_id = cursor.lastrowid
            for i, (idx, _) in enumerate(push_events):
                payload_id_map[idx] = start_id + i
        
        if watch_events:
            sql = "INSERT INTO payload_star (action, star_repo_id) VALUES (%s, %s)"
            values = [('started', repo_id) for idx, repo_id in watch_events]
            cursor.executemany(sql, values)
            start_id = cursor.lastrowid
            for i, (idx, _) in enumerate(watch_events):
                payload_id_map[idx] = start_id + i
        
        if fork_events:
            sql = "INSERT INTO payload_fork (forkee_id, forkee_name) VALUES (%s, %s)"
            values = [(p.get('forkee', {}).get('id'), (p.get('forkee', {}).get('full_name') or '')[:255])
                     for idx, p in fork_events]
            cursor.executemany(sql, values)
            start_id = cursor.lastrowid
            for i, (idx, _) in enumerate(fork_events):
                payload_id_map[idx] = start_id + i
        
        if create_events:
            sql = "INSERT INTO payload_create (ref, ref_type, description) VALUES (%s, %s, %s)"
            values = [((p.get('ref') or '')[:255], (p.get('ref_type') or '')[:20], p.get('description'))
                     for idx, p in create_events]
            cursor.executemany(sql, values)
            start_id = cursor.lastrowid
            for i, (idx, _) in enumerate(create_events):
                payload_id_map[idx] = start_id + i
        
        logger.info(f"    插入 {len(payload_id_map)} 个Payload")
        return payload_id_map
    
    def _bulk_insert_events_safe(self, cursor, events: List[Dict], payload_map: Dict):
        """批量插入事件（应用层验证）"""
        sql = """
            INSERT IGNORE INTO events (
                gh_event_id, event_type, public, created_at, created_at_date,
                actor_id, repo_id, org_id, payload_id, actor_login, repo_name
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        values = []
        for idx, event in enumerate(events):
            try:
                actor_id = event.get('actor', {}).get('id')
                repo_id = event.get('repo', {}).get('id')
                
                if not actor_id or not repo_id:
                    self.stats['skipped'] += 1
                    continue
                
                if actor_id not in self.existing_actors or repo_id not in self.existing_repos:
                    self.stats['skipped'] += 1
                    continue
                
                created_at = event.get('created_at', datetime.now().isoformat())
                created_dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                
                org_id = event.get('org', {}).get('id')
                if org_id and org_id not in self.existing_orgs:
                    org_id = None
                
                values.append((
                    event.get('id'),
                    event.get('type', '')[:50],
                    1 if event.get('public') else 0,
                    created_dt,
                    created_dt.date(),
                    actor_id,
                    repo_id,
                    org_id,
                    payload_map.get(idx, 1),
                    event.get('actor', {}).get('login', '')[:100],
                    event.get('repo', {}).get('name', '')[:255]
                ))
            except:
                self.stats['skipped'] += 1
                continue
        
        batch_size = 1000
        total = 0
        for i in range(0, len(values), batch_size):
            batch = values[i:i+batch_size]
            cursor.executemany(sql, batch)
            total += cursor.rowcount
        
        self.stats['events_inserted'] = total
        logger.info(f"    插入 {total} 条事件，跳过 {self.stats['skipped']} 条")
    
    def _print_stats(self):
        logger.info("=" * 60)
        logger.info("处理统计:")
        logger.info(f"  插入事件: {self.stats['events_inserted']}")
        logger.info(f"  新增用户: {self.stats['actors_inserted']}")
        logger.info(f"  新增仓库: {self.stats['repos_inserted']}")
        logger.info(f"  跳过: {self.stats['skipped']}")
        logger.info("=" * 60)
    
    def ingest_hour(self, year: int, month: int, day: int, hour: int):
        """处理单个小时"""
        self.stats = {k: 0 for k in self.stats}
        self.stream_download_and_process(year, month, day, hour)
    
    def ingest_day(self, year: int, month: int, day: int):
        """处理一整天"""
        logger.info(f"开始处理 {year}-{month:02d}-{day:02d} 全天数据")
        
        for hour in range(24):
            try:
                self.ingest_hour(year, month, day, hour)
            except Exception as e:
                logger.error(f"处理 {hour:02d}:00 失败: {e}")
                continue


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='GH Archive数据摄取脚本')
    parser.add_argument('date', type=str, help='日期时间，格式: YYYY-MM-DD-HH (例如: 2025-12-24-15) 或 YYYY-MM-DD (处理整天)')
    
    args = parser.parse_args()
    
    # 解析日期字符串
    date_parts = args.date.split('-')
    
    if len(date_parts) == 4:
        # 处理单个小时: YYYY-MM-DD-HH
        try:
            year = int(date_parts[0])
            month = int(date_parts[1])
            day = int(date_parts[2])
            hour = int(date_parts[3])
            
            if 0 <= hour <= 23:
                ingestor = DualConnectionIngestor()
                ingestor.ingest_hour(year, month, day, hour)
            else:
                print("错误: 小时必须在 0-23 之间")
                exit(1)
        except ValueError:
            print("错误: 日期格式不正确，请使用 YYYY-MM-DD-HH 格式")
            exit(1)
    elif len(date_parts) == 3:
        # 处理整天: YYYY-MM-DD
        try:
            year = int(date_parts[0])
            month = int(date_parts[1])
            day = int(date_parts[2])
            
            ingestor = DualConnectionIngestor()
            ingestor.ingest_day(year, month, day)
        except ValueError:
            print("错误: 日期格式不正确，请使用 YYYY-MM-DD 格式")
            exit(1)
    else:
        print("错误: 日期格式不正确，请使用 YYYY-MM-DD-HH (单个小时) 或 YYYY-MM-DD (整天) 格式")
        exit(1)