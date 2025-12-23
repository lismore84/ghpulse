#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试 API 返回的数据结构
"""

import requests
import json

print("=" * 70)
print("测试热门仓库 API")
print("=" * 70)

try:
    response = requests.get('http://localhost:5000/api/trending/repos?limit=10')
    print(f"HTTP 状态码: {response.status_code}\n")
    data = response.json()
    print("完整响应:")
    print(json.dumps(data, indent=2, ensure_ascii=False))
    
    print("\n" + "-" * 70)
    if 'data' in data and len(data['data']) > 0:
        print("第一条数据的字段:")
        first_item = data['data'][0]
        for key, value in first_item.items():
            print(f"  {key:25s} = {value}")
except Exception as e:
    print(f"❌ 请求失败: {e}")

print("\n" + "=" * 70)
print("测试活跃开发者 API")
print("=" * 70)

try:
    response = requests.get('http://localhost:5000/api/trending/developers?limit=10')
    print(f"HTTP 状态码: {response.status_code}\n")
    data = response.json()
    print("完整响应:")
    print(json.dumps(data, indent=2, ensure_ascii=False))
    
    print("\n" + "-" * 70)
    if 'data' in data and len(data['data']) > 0:
        print("第一条数据的字段:")
        first_item = data['data'][0]
        for key, value in first_item.items():
            print(f"  {key:25s} = {value}")
except Exception as e:
    print(f"❌ 请求失败: {e}")