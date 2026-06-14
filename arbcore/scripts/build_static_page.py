import os
import sys
import json
from datetime import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from arbcore.database.managers.db_manager import DatabaseManager
from ArbDashboard.backend.services.fund_service import FundService

def build():
    print("⏳ 开始生成静态看板数据...")
    db = DatabaseManager()
    fund_service = FundService(db)
    
    # 模拟不需要实时行情的纯静态数据
    # 这里其实直接调用 get_unified_dashboard_data 即可
    data = fund_service.get_unified_dashboard_data()
    
    # 按分类聚合
    categories = {}
    for item in data:
        cat = item.get('category', '其他')
        if cat not in categories:
            categories[cat] = []
        categories[cat].append(item)
        
    json_data = json.dumps(categories, ensure_ascii=False)
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    html_template = f"""<!DOCTYPE html>
<html lang="zh-CN" class="dark">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>ArbTest 静态看板 - {timestamp}</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script src="https://unpkg.com/vue@3/dist/vue.global.prod.js"></script>
    <style>
        body {{ background-color: #121212; color: #E0E0E0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif; }}
        .glass-panel {{ background: rgba(30, 30, 30, 0.6); backdrop-filter: blur(10px); border: 1px solid rgba(255, 255, 255, 0.05); border-radius: 8px; }}
        .tab-btn {{ padding: 0.5rem 1rem; border-radius: 6px; cursor: pointer; transition: all 0.2s; }}
        .tab-btn.active {{ background: #18a058; color: white; }}
        .tab-btn:hover:not(.active) {{ background: rgba(255, 255, 255, 0.1); }}
        table {{ width: 100%; border-collapse: collapse; text-align: right; }}
        th, td {{ padding: 12px 8px; border-bottom: 1px solid rgba(255,255,255,0.05); font-size: 14px; }}
        th {{ color: #888; font-weight: normal; }}
        tr:hover {{ background: rgba(255, 255, 255, 0.02); }}
        .text-up {{ color: #f5222d; }}
        .text-down {{ color: #52c41a; }}
        .text-left {{ text-align: left; }}
    </style>
</head>
<body>
    <div id="app" class="p-6 max-w-7xl mx-auto">
        <div class="flex justify-between items-center mb-6">
            <h1 class="text-2xl font-bold flex items-center gap-2">
                <span class="text-green-500">📊</span> ArbTest 静态大屏
            </h1>
            <div class="text-sm text-gray-400">数据快照时间: {timestamp} (静态演示模式)</div>
        </div>

        <div class="glass-panel p-4 mb-6">
            <div class="flex gap-2 mb-4 overflow-x-auto pb-2">
                <div v-for="cat in Object.keys(marketData)" :key="cat"
                     @click="activeTab = cat"
                     :class="['tab-btn whitespace-nowrap', activeTab === cat ? 'active' : 'text-gray-400']">
                    {{{{ cat }}}} ({{{{ marketData[cat].length }}}})
                </div>
            </div>

            <div class="overflow-x-auto">
                <table>
                    <thead>
                        <tr>
                            <th class="text-left">代码</th>
                            <th class="text-left">名称</th>
                            <th>静态估值</th>
                            <th>静态溢价</th>
                            <th>场内份额(万)</th>
                            <th>T-1日期</th>
                            <th>T-2折溢率</th>
                            <th>T-3折溢率</th>
                            <th>申赎状态</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr v-for="row in currentData" :key="row.fund_code">
                            <td class="text-left text-gray-400">{{{{ row.fund_code }}}}</td>
                            <td class="text-left font-medium">{{{{ row.fund_name }}}}</td>
                            <td>{{{{ formatNumber(row.static_val, 3) }}}}</td>
                            <td :class="getColor(row.static_premium)">{{{{ formatNumber(row.static_premium, 2) }}}}%</td>
                            <td>{{{{ formatNumber(row.shares) }}}}</td>
                            <td>{{{{ row.nav_date }}}}</td>
                            <td :class="getColor(row.t_2_premium)">{{{{ formatNumber(row.t_2_premium, 2) }}}}%</td>
                            <td :class="getColor(row.t_3_premium)">{{{{ formatNumber(row.t_3_premium, 2) }}}}%</td>
                            <td class="text-xs">
                                <span class="bg-gray-800 px-2 py-1 rounded">{{{{ row.purchase_status }}}}</span>
                            </td>
                        </tr>
                    </tbody>
                </table>
            </div>
        </div>
    </div>

    <script>
        const {{ createApp, ref, computed }} = Vue

        createApp({{
            setup() {{
                const marketData = ref({json_data})
                const activeTab = ref(Object.keys(marketData.value)[0])

                const currentData = computed(() => {{
                    return marketData.value[activeTab.value] || []
                }})

                const formatNumber = (val, decimals = 2) => {{
                    if (val === null || val === undefined || isNaN(val) || val === 0) return '-';
                    return Number(val).toFixed(decimals);
                }}

                const getColor = (val) => {{
                    if (!val || isNaN(val) || val === 0) return '';
                    return Number(val) > 0 ? 'text-up' : 'text-down';
                }}

                return {{
                    marketData,
                    activeTab,
                    currentData,
                    formatNumber,
                    getColor
                }}
            }}
        }}).mount('#app')
    </script>
</body>
</html>
"""
    
    out_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'jsl', 'jslvercel')
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "index.html")
    
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html_template)
        
    print(f"✅ 静态看板页面已生成: {out_path}")
    return out_path

if __name__ == '__main__':
    build()
