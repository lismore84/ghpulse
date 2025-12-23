# GitHub Pulse (GHPulse)

GHPulse 是一个实时 GitHub 活动监控和分析平台，提供热门仓库和活跃开发者的实时榜单，帮助用户追踪 GitHub 上的最新趋势和活动。

## 功能特性

- **热门仓库榜单**: 实时显示 GitHub 上最活跃的仓库，包括星标、Fork、PR 数量等指标
- **活跃开发者榜单**: 展示 GitHub 上最活跃的开发者，包括提交、PR、Issue 数量等指标
- **数据可视化**: 通过图表展示 GitHub 事件类型的统计分析
- **自定义查询**: 支持自定义查询 GitHub 事件数据
- **实时数据更新**: 通过 ETL 流程持续更新数据

## 技术架构

- **后端**: Python Flask Web 框架
- **前端**: Vue.js 3 + Element Plus UI 组件库
- **数据库**: MySQL 8.0+
- **数据采集**: GitHub Events API 实时流式采集
- **缓存**: 内存缓存优化查询性能

## 项目结构

```
ghpulse/
├── .gitignore
├── README.md            # 项目说明文档
├── db_init/             # 数据库初始化脚本目录
│   ├── db_ack.sql       # 数据库确认脚本
│   ├── db_init.sql      # 数据库初始化脚本
│   ├── db_user_init.sql # 数据库用户初始化脚本
│   └── db_user_init_example.sql # 数据库用户初始化示例脚本
├── ghpulse_etl/         # 数据提取、转换、加载模块
│   ├── streaming_ingest.py  # 实时数据采集
│   └── update_all_stats.py  # 统计数据更新
├── ghpulse_web/         # Web 应用主目录
│   ├── app.py           # Flask Web 应用主入口
│   ├── static/          # 静态资源
│   │   ├── app.js       # Vue.js 前端应用
│   │   └── style.css    # 自定义样式
│   └── templates/       # HTML 模板
│       └── index.html   # 主页面模板
├── requirements.txt     # 项目依赖
└── test_api.py          # API 测试脚本
```

## 安装与运行

### 环境要求

- Python 3.8+
- pip 包管理器
- Git 
- MySQL 8.0+ 数据库

### 安装步骤

1. **克隆项目**
   ```bash
   git clone https://github.com/Youtju/ghpulse.git
   cd ghpulse
   ```

2. **准备云数据库**
   - 注册并创建 MySQL 数据库实例
   - 记录数据库连接信息 (主机公网IP、端口、用户名、密码、数据库名)

3. **执行数据库初始化脚本**
   - 登录云数据库控制台
   - 执行 `db_init.sql` 脚本初始化数据库结构

4. **补全用户权限脚本**
   - 编辑 `db_init/db_user_init_example.sql` 文件
   - 替换 `your_ingest_password`, `your_web_password`, `your_admin_password` 为实际密码
   - 云数据库控制台执行 `db_init/db_user_init_example.sql` 脚本初始化用户权限

5. **安装依赖**
   ```bash
   pip install -r requirements.txt
   ```
```
6. **配置环境变量**
   - 在 `ghpulse_etl/` 和 `ghpulse_web/` 目录下创建 `.env` 文件
   - 编辑 `.env` 文件，填写数据库连接信息
   ```
      # 数据库配置
      DB_HOST=your_host_ip
      DB_PORT=your_port
      DB_NAME=ghpulse

      # 数据写入用户（用于ETL）
      DB_USER=ingest_user
      DB_PASSWORD=your_ingest_password

      # 管理员用户（用于触发器管理）
      ADMIN_USER=admin_user
      ADMIN_PASSWORD=your_admin_password

      # Web只读用户（备用）
      WEB_DB_USER=web_user
      WEB_DB_PASSWORD=your_web_password
   ```

7. **启动数据采集服务**
   ```bash
   python ghpulse_etl/streaming_ingest.py
   ```

8. **启动 Web 服务**
   ```bash
   cd ghpulse_web
   python app.py
   ```
9. **浏览器访问**
   - 打开浏览器，访问 `http://localhost:5000`


### 数据查询接口

- `GET /api/query` - 执行自定义 SQL 查询
- `GET /api/stats/total` - 获取总统计信息
- `GET /api/stats/event_types` - 获取事件类型统计
- `GET /api/trending/repos?limit=10` - 获取热门仓库榜单
- `GET /api/trending/developers?limit=10` - 获取活跃开发者榜单

### 管理接口

- `POST /api/update_stats` - 手动更新统计数据
- `GET /api/health` - 健康检查接口

## 使用说明
### 功能模块
1. **查询模块** - 使用自定义 SQL 查询数据
2. **热门榜单** - 查看热门仓库和活跃开发者
3. **统计分析** - 查看事件类型分布和趋势

### 数据更新
```bash
python ghpulse_etl/streaming_ingest.py --start-date 2025-01-01 --end-date 2025-01-07
python ghpulse_etl/update_all_stats.py
```

## 开发说明
### 前端开发
前端使用 Vue.js 3 和 Element Plus，主要代码在 `ghpulse_web/static/app.js` 中。

### 后端开发
后端使用 Flask 框架，主要代码在 `ghpulse_web/app.py` 中，包括路由定义和数据处理逻辑。

### 数据采集
数据采集使用 GitHub Events API，实现实时数据流处理，代码在 `ghpulse_etl/` 目录下。

## 故障排除
### 常见问题
1. **页面无法加载**
   - 确认 Flask 服务已启动
   - 检查端口 5000 是否被占用
2. **数据为空**
   - 检查数据采集服务是否运行
   - 确认数据库连接正常

### 日志查看
- Web 应用日志: 终端输出
- 数据采集日志: `ghpulse_etl/` 目录下的日志文件

## 贡献指南
github仓库地址 https://github.com/Youtju/ghpulse.git
欢迎提交 Issue 和 Pull Request 来改进项目。