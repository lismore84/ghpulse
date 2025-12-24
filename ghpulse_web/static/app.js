const { createApp, ref, reactive, onMounted, nextTick } = Vue;

// API基础URL（相对路径）
const API_BASE_URL = '/api';

// 创建Vue应用
const app = createApp({
    setup() {
        // 状态管理
        const loading = ref(false);
        const activeTab = ref('query');
        const activeView = ref('query');
        const currentTable = ref('');
        
        // 数据
        const tables = ref([]);
        const tableData = ref([]);
        const tableColumns = ref([]);
        const pagination = reactive({
            page: 1,
            page_size: 20,
            total: 0,
            total_pages: 0
        });
        
        const stats = ref({
            total_events: 0,
            total_actors: 0,
            total_repos: 0,
            total_orgs: 0,
            latest_event: null
        });
        
        const customSQL = ref('');
        const queryResult = reactive({
            rows: null,
            columns: [],
            count: 0,
            execution_time: 0
        });
        
        const trendingRepos = ref([]);
        const trendingDevelopers = ref([]);
        const eventTypeStats = ref([]);

        const trendingReposTableRef = ref(null);
        const trendingDevelopersTableRef = ref(null);

        const relayoutTrendingTables = async () => {
            await nextTick();
            trendingReposTableRef.value?.doLayout?.();
            trendingDevelopersTableRef.value?.doLayout?.();
        };
        
        // API请求封装
        const api = {
            async get(url) {
                const response = await axios.get(`${API_BASE_URL}${url}`);
                return response.data;
            },
            async post(url, data) {
                const response = await axios.post(`${API_BASE_URL}${url}`, data);
                return response.data;
            }
        };
        
        // ==================== 数据加载函数 ====================
        
        // 加载表列表
        const loadTables = async () => {
            try {
                loading.value = true;
                const result = await api.get('/tables');
                if (result.success) {
                    tables.value = result.data;
                    console.log(`加载了 ${result.data.length} 个表`);
                }
            } catch (error) {
                console.error('加载表列表失败:', error);
                ElMessage.error('加载表列表失败: ' + error.message);
            } finally {
                loading.value = false;
            }
        };
        
        // 加载统计数据
        const loadStats = async () => {
            try {
                const result = await api.get('/stats/overview');
                if (result.success) {
                    stats.value = result.data;
                    console.log('统计数据加载成功:', result.data);
                }
            } catch (error) {
                console.error('加载统计失败:', error);
            }
        };
        
        // 加载表数据
        const loadTableData = async () => {
            if (!currentTable.value) return;
            
            try {
                loading.value = true;
                const result = await api.get(
                    `/table/${currentTable.value}?page=${pagination.page}&page_size=${pagination.page_size}`
                );
                
                if (result.success) {
                    tableData.value = result.data.rows;
                    tableColumns.value = result.data.columns;
                    Object.assign(pagination, result.data.pagination);
                    console.log(`加载表 ${currentTable.value}:`, result.data.rows.length, '行');
                }
            } catch (error) {
                console.error('加载数据失败:', error);
                ElMessage.error('加载数据失败: ' + error.message);
            } finally {
                loading.value = false;
            }
        };
        
        // 执行自定义查询
        const executeQuery = async () => {
            if (!customSQL.value.trim()) {
                ElMessage.warning('请输入SQL查询语句');
                return;
            }
            
            try {
                loading.value = true;
                const result = await api.post('/query', { sql: customSQL.value });
                
                if (result.success) {
                    queryResult.rows = result.data.rows;
                    queryResult.columns = result.data.columns;
                    queryResult.count = result.data.count;
                    queryResult.execution_time = result.data.execution_time.toFixed(3);
                    console.log(`查询成功，返回 ${result.data.count} 行`);
                    ElMessage.success(`查询成功，返回 ${result.data.count} 行数据`);
                } else {
                    ElMessage.error('查询失败: ' + result.error);
                }
            } catch (error) {
                console.error('查询失败:', error);
                const errorMsg = error.response?.data?.error || error.message;
                ElMessage.error('查询失败: ' + errorMsg);
            } finally {
                loading.value = false;
            }
        };
        
        // 加载热门仓库
        const loadTrendingRepos = async () => {
            try {
                loading.value = true;
                const result = await api.get('/trending/repos?limit=10');
                if (result.success) {
                    trendingRepos.value = result.data;
                    console.log(`加载热门仓库: ${result.data.length} 个`, result.source);
                    console.log('热门仓库数据详情:', JSON.stringify(result.data.slice(0, 2), null, 2));
                    if (result.data.length === 0) {
                        ElMessage.warning('暂无热门仓库数据');
                    }
                    if (activeView.value === 'trending') {
                        await relayoutTrendingTables();
                    }
                }
            } catch (error) {
                console.error('加载热门仓库失败:', error);
                ElMessage.error('加载热门仓库失败');
            } finally {
                loading.value = false;
            }
        };
        
        // 加载活跃开发者
        const loadTrendingDevelopers = async () => {
            try {
                loading.value = true;
                const result = await api.get('/trending/developers?limit=10');
                if (result.success) {
                    trendingDevelopers.value = result.data;
                    console.log(`加载活跃开发者: ${result.data.length} 个`, result.source);
                    console.log('活跃开发者数据详情:', JSON.stringify(result.data.slice(0, 2), null, 2));
                    if (result.data.length === 0) {
                        ElMessage.warning('暂无活跃开发者数据');
                    }
                    if (activeView.value === 'trending') {
                        await relayoutTrendingTables();
                    }
                }
            } catch (error) {
                console.error('加载活跃开发者失败:', error);
                ElMessage.error('加载活跃开发者失败');
            } finally {
                loading.value = false;
            }
        };
        
        // 加载事件类型统计
        const loadEventTypeStats = async () => {
            try {
                loading.value = true;
                const result = await api.get('/stats/event_types');
                if (result.success) {
                    eventTypeStats.value = result.data;
                    console.log(`加载事件类型统计: ${result.data.length} 种类型`);
                    // 打印前3个，检查数据结构
                    if (result.data.length > 0) {
                        console.log('事件类型示例:', result.data.slice(0, 3));
                    }
                }
            } catch (error) {
                console.error('加载事件类型统计失败:', error);
                ElMessage.error('加载事件类型统计失败');
            } finally {
                loading.value = false;
            }
        };
        
        // ==================== 事件处理 ====================
        
        // 菜单选择处理
        const handleMenuSelect = (index) => {
            activeTab.value = index;
            console.log('切换到:', index);
            
            if (index.startsWith('table-')) {
                const tableName = index.replace('table-', '');
                currentTable.value = tableName;
                activeView.value = 'table';
                pagination.page = 1;
                loadTableData();
            } else if (index === 'query') {
                activeView.value = 'query';
            } else if (index === 'trending') {
                activeView.value = 'trending';
                loadTrendingRepos();
                loadTrendingDevelopers();
                relayoutTrendingTables();
            } else if (index === 'stats') {
                activeView.value = 'stats';
                loadEventTypeStats();
            }
        };
        
        // 分页处理
        const handlePageChange = (page) => {
            pagination.page = page;
            loadTableData();
        };
        
        const handleSizeChange = (size) => {
            pagination.page_size = size;
            pagination.page = 1;
            loadTableData();
        };
        
        // 导出数据
        const exportData = () => {
            if (!tableData.value || tableData.value.length === 0) {
                ElMessage.warning('没有数据可导出');
                return;
            }
            
            // 简单的CSV导出
            try {
                const headers = tableColumns.value.map(col => col.field);
                const csvContent = [
                    headers.join(','),
                    ...tableData.value.map(row => 
                        headers.map(h => {
                            const value = row[h];
                            // 处理包含逗号的值
                            return typeof value === 'string' && value.includes(',') 
                                ? `"${value}"` 
                                : value;
                        }).join(',')
                    )
                ].join('\n');
                
                const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
                const link = document.createElement('a');
                link.href = URL.createObjectURL(blob);
                link.download = `${currentTable.value}_${new Date().toISOString().slice(0,10)}.csv`;
                link.click();
                
                ElMessage.success('导出成功');
            } catch (error) {
                console.error('导出失败:', error);
                ElMessage.error('导出失败: ' + error.message);
            }
        };
        
        // ==================== 工具函数 ====================
        
        // 格式化数字
        const formatNumber = (num) => {
            if (num === null || num === undefined) return '0';
            const n = Number(num);
            if (isNaN(n)) return '0';
            if (n >= 1000000) return (n / 1000000).toFixed(1) + 'M';
            if (n >= 1000) return (n / 1000).toFixed(1) + 'K';
            return n.toLocaleString();
        };
        
        // 获取列宽度
        const getColumnWidth = (type) => {
            if (!type) return 150;
            const t = type.toLowerCase();
            if (t.includes('bigint')) return 150;
            if (t.includes('int')) return 120;
            if (t.includes('datetime')) return 180;
            if (t.includes('timestamp')) return 180;
            if (t.includes('date')) return 120;
            if (t.includes('varchar(100)')) return 150;
            if (t.includes('text')) return 300;
            return 200;
        };
        
        // 获取统计标签
        const getStatLabel = (key) => {
            const labels = {
                total_events: '总事件数',
                total_actors: '总用户数',
                total_repos: '总仓库数',
                total_orgs: '总组织数',
                latest_event: '最新事件时间'
            };
            return labels[key] || key;
        };
        
        // 格式化统计值
        const formatStatValue = (key, value) => {
            if (value === null || value === undefined) return '-';
            
            if (key === 'latest_event') {
                if (!value) return '-';
                try {
                    return new Date(value).toLocaleString('zh-CN', {
                        year: 'numeric',
                        month: '2-digit',
                        day: '2-digit',
                        hour: '2-digit',
                        minute: '2-digit'
                    });
                } catch (error) {
                    return value;
                }
            }
            
            const n = Number(value);
            if (isNaN(n)) return value;
            return n.toLocaleString();
        };
        
        // 计算百分比
        const getPercentage = (count) => {
            const total = eventTypeStats.value.reduce((sum, item) => sum + (item.count || 0), 0);
            if (total === 0) return 0;
            const percentage = ((count / total) * 100);
            return Math.round(percentage * 100) / 100; // 保留2位小数
        };
        
        // 获取进度条颜色
        const getProgressColor = (index) => {
            const colors = ['#409EFF', '#67C23A', '#E6A23C', '#F56C6C', '#909399'];
            return colors[index % colors.length];
        };
        
        // ==================== 生命周期 ====================
        
        onMounted(() => {
            console.log('GHPulse Web 应用已挂载');
            loadTables();
            loadStats();
            // 默认加载事件类型统计
            loadEventTypeStats();
            // 加载热门榜单数据
            loadTrendingRepos();
            loadTrendingDevelopers();
        });
        
        // ==================== 返回 ====================
        
        return {
            // 状态
            loading,
            activeTab,
            activeView,
            currentTable,
            
            // 数据
            tables,
            tableData,
            tableColumns,
            pagination,
            stats,
            customSQL,
            queryResult,
            trendingRepos,
            trendingDevelopers,
            eventTypeStats,
            trendingReposTableRef,
            trendingDevelopersTableRef,
            
            // 方法
            handleMenuSelect,
            handlePageChange,
            handleSizeChange,
            loadTableData,
            executeQuery,
            loadTrendingRepos,
            loadTrendingDevelopers,
            loadEventTypeStats,
            exportData,
            relayoutTrendingTables,
            
            // 工具函数
            formatNumber,
            getColumnWidth,
            getStatLabel,
            formatStatValue,
            getPercentage,
            getProgressColor
        };
    }
});

// 注册Element Plus所有图标
for (const [key, component] of Object.entries(ElementPlusIconsVue)) {
    app.component(key, component);
}

// 使用Element Plus
app.use(ElementPlus);

// 挂载应用
app.mount('#app');

console.log('GHPulse Web 应用启动成功');
