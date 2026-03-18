import subprocess
import json
import asyncio
import aiohttp
import re
import os
import random
from collections import defaultdict
from datetime import datetime

# --- 配置参数 ---
CONCURRENT_CHECKS = 30
FAST_CHECK_TIMEOUT = 5
MIN_BITRATE = 200
OUTPUT_FILE = "live.m3u"
INPUT_SOURCE = "live.txt"

# 酒店源配置
HOTEL_SOURCE_URL = "https://raw.githubusercontent.com/gclgg/zubo/main/itvlist.txt"
HOTEL_MAIN_GROUP = "酒店源"

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.36',
]

EPG_URLS = [
    "http://epg.112114.xyz/pp.xml",
    "https://epg.112114.free.hr/pp.xml",
]

def clean_group_name(group_name):
    """清理分组名称，去掉逗号"""
    return re.sub(r',', '', group_name).strip()

def extract_logo_from_m3u(channel_name, m3u_file):
    """从原始 M3U 文件中提取 logo"""
    if not os.path.exists(m3u_file):
        return ""
    with open(m3u_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    for line in lines:
        if line.startswith('#EXTINF') and f',{channel_name}' in line:
            logo_match = re.search(r'tvg-logo="([^"]+)"', line)
            if logo_match:
                return logo_match.group(1)
    return ""

def parse_txt_file(filename):
    """解析本地直播源 TXT 文件"""
    channels_by_group = defaultdict(list)
    current_group = "未分组"
    m3u_file = filename.replace('.txt', '.m3u')
    
    with open(filename, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            if line.endswith('#genre#'):
                # 清理分组名中的逗号
                current_group = clean_group_name(line[:-7].strip())
                continue
            if ',' in line:
                parts = line.split(',', 1)
                channel_name = parts[0].strip()
                full_url = parts[1].strip()
                # 提取纯净URL（去掉$后面的参数）
                clean_url = re.sub(r'\$.*$', '', full_url)
                logo_url = extract_logo_from_m3u(channel_name, m3u_file)
                
                channels_by_group[current_group].append({
                    'name': channel_name,
                    'full_url': full_url,
                    'clean_url': clean_url,
                    'logo': logo_url,
                    'is_announcement': current_group == '公告'
                })
    
    return dict(channels_by_group)

async def fetch_hotel_source():
    """拉取酒店源，保持原始分组结构"""
    print(f"\n🏨 正在拉取酒店源: {HOTEL_SOURCE_URL}")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(HOTEL_SOURCE_URL, timeout=30) as resp:
                if resp.status != 200:
                    print(f"❌ 拉取失败: HTTP {resp.status}")
                    return {}
                
                content = await resp.text()
                hotel_by_subgroup = defaultdict(list)
                current_subgroup = None
                lines = content.strip().split('\n')
                
                for line in lines:
                    line = line.strip()
                    if not line:
                        continue
                    
                    # 处理分组行（去掉逗号）
                    if line.endswith('#genre#'):
                        current_subgroup = clean_group_name(line[:-7].strip())
                        continue
                    
                    # 处理频道行
                    if ',' in line and current_subgroup:
                        parts = line.split(',', 1)
                        channel_name = parts[0].strip()
                        channel_url = parts[1].strip()
                        
                        hotel_by_subgroup[current_subgroup].append({
                            'name': channel_name,
                            'url': channel_url
                        })
                
                # 统计并显示
                total = sum(len(ch) for ch in hotel_by_subgroup.values())
                print(f"✅ 拉取成功，{len(hotel_by_subgroup)} 个子分组，{total} 个频道")
                for subgroup, channels in hotel_by_subgroup.items():
                    print(f"   - {subgroup}: {len(channels)} 个频道")
                
                return dict(hotel_by_subgroup)
    except Exception as e:
        print(f"❌ 拉取失败: {e}")
        return {}

async def fast_check(session, clean_url):
    """快速 HEAD 检查，判断源是否可访问"""
    try:
        async with session.head(clean_url, timeout=FAST_CHECK_TIMEOUT, allow_redirects=True) as resp:
            # 200、301、302、307、308 都视为可访问
            is_valid = resp.status in [200, 301, 302, 307, 308]
            if is_valid:
                print(f"  ✅ {resp.status}")
            else:
                print(f"  ❌ {resp.status}")
            return is_valid
    except Exception as e:
        error_type = type(e).__name__
        print(f"  ⚠️ {error_type}")
        # ServerDisconnectedError 和超时可能仍是有效的
        if 'ServerDisconnectedError' in error_type or 'Timeout' in error_type:
            return True
        return False

async def check_channel(session, channel):
    """检测单个频道，放宽判断标准"""
    if channel.get('is_announcement'):
        return {
            'name': channel['name'],
            'group': channel['group'],
            'full_url': channel['full_url'],
            'logo': channel.get('logo', ''),
            'valid': True,
            'height': 1080
        }
    
    clean_url = channel['clean_url']
    
    # 打印检测中的频道
    print(f"检测: {channel['name']} - {clean_url[:60]}...")
    
    # RTSP 流直接视为有效（难以快速检测）
    if clean_url.startswith('rtsp://'):
        print(f"  📹 RTSP流（直接接受）")
        return {
            'name': channel['name'],
            'group': channel['group'],
            'full_url': channel['full_url'],
            'logo': channel.get('logo', ''),
            'valid': True,
            'height': 720
        }
    
    # HTTP/HTTPS 流进行检测
    try:
        if await fast_check(session, clean_url):
            return {
                'name': channel['name'],
                'group': channel['group'],
                'full_url': channel['full_url'],
                'logo': channel.get('logo', ''),
                'valid': True,
                'height': 720
            }
    except Exception as e:
        # 异常时也接受，避免误判
        print(f"  ⚠️ 异常但仍接受")
        return {
            'name': channel['name'],
            'group': channel['group'],
            'full_url': channel['full_url'],
            'logo': channel.get('logo', ''),
            'valid': True,
            'height': 720
        }
    
    return None

async def main():
    import time
    start_time = time.time()
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    print(f"\n🕐 当前时间: {current_time}")
    
    # 1. 拉取酒店源
    hotel_data = await fetch_hotel_source()
    
    # 2. 解析本地源
    if not os.path.exists(INPUT_SOURCE):
        print(f"错误：文件 {INPUT_SOURCE} 不存在！")
        return
    
    channels_by_group = parse_txt_file(INPUT_SOURCE)
    
    # 3. 分离公告和频道
    announcement = None
    channels_to_check = []
    
    for group, channels in channels_by_group.items():
        for channel in channels:
            if group == '公告' and '更新日期' in channel['name']:
                announcement = channel
            elif group != '公告':
                channels_to_check.append(channel)
    
    print(f"\n📢 公告: 1 条")
    print(f"📺 需要检测的本地频道: {len(channels_to_check)} 个")
    
    # 4. 检测本地频道
    valid_channels = []
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(ssl=False),
        headers={'User-Agent': random.choice(USER_AGENTS)}
    ) as session:
        semaphore = asyncio.Semaphore(CONCURRENT_CHECKS)
        
        async def bounded_check(ch):
            async with semaphore:
                return await check_channel(session, ch)
        
        tasks = [bounded_check(ch) for ch in channels_to_check]
        results = await asyncio.gather(*tasks)
    
    valid_channels = [r for r in results if r]
    print(f"\n✅ 本地频道检测完成！有效: {len(valid_channels)}")
    
    # 5. 按分组整理本地有效源
    local_by_group = defaultdict(list)
    for ch in valid_channels:
        local_by_group[ch['group']].append(ch)
    
    # 6. 写入最终的 M3U 文件
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        # 写入 EPG 信息行
        f.write('#EXTM3U x-tvg-url="' + '","'.join(EPG_URLS) + '"\n')
        
        # === 第一部分：公告 ===
        if announcement:
            f.write('\n# 分组：公告\n')
            announcement_name = f"更新日期 {current_time}"
            tvg_id = str(abs(hash(announcement_name)) % 10000)
            extinf = f'#EXTINF:-1 tvg-id="{tvg_id}" tvg-name="{announcement_name}"'
            if announcement.get('logo'):
                extinf += f' tvg-logo="{announcement["logo"]}"'
            extinf += f' group-title="公告",{announcement_name}'
            f.write(extinf + '\n')
            f.write(announcement['full_url'] + '\n')
        
        # === 第二部分：本地有效频道（按原分组） ===
        if local_by_group:
            f.write('\n# ========== 本地源 ==========\n')
            for group, channels in local_by_group.items():
                if channels:
                    f.write(f'\n# 分组：{group}\n')
                    for ch in channels:
                        tvg_id = str(abs(hash(ch['name'])) % 10000)
                        extinf = f'#EXTINF:-1 tvg-id="{tvg_id}" tvg-name="{ch["name"]}"'
                        if ch.get('logo'):
                            extinf += f' tvg-logo="{ch["logo"]}"'
                        extinf += f' group-title="{group}",{ch["name"]}'
                        f.write(extinf + '\n')
                        f.write(ch['full_url'] + '\n')
        
        # === 第三部分：酒店源（保持子分组结构） ===
        if hotel_data:
            f.write(f'\n# ========== {HOTEL_MAIN_GROUP} ==========\n')
            for subgroup, channels in hotel_data.items():
                if channels:
                    # 每个子分组作为一个独立分组显示
                    f.write(f'\n# 分组：{subgroup}\n')
                    for ch in channels:
                        tvg_id = str(abs(hash(ch['name'])) % 10000)
                        # 注意：group-title 使用子分组名，这样播放器会按子分组显示
                        extinf = f'#EXTINF:-1 tvg-id="{tvg_id}" tvg-name="{ch["name"]}" group-title="{subgroup}",{ch["name"]}'
                        f.write(extinf + '\n')
                        f.write(ch['url'] + '\n')
    
    # 统计信息
    total_hotel = sum(len(ch) for ch in hotel_data.values()) if hotel_data else 0
    elapsed = time.time() - start_time
    
    print(f"\n⏱️ 总耗时: {elapsed:.1f} 秒")
    print(f"🕐 更新时间: {current_time}")
    print(f"\n📊 最终文件统计:")
    print(f"  - 公告: 1 条")
    print(f"  - 本地有效源: {len(valid_channels)} 个")
    if hotel_data:
        print(f"  - 酒店源: {total_hotel} 个频道，{len(hotel_data)} 个子分组")
        for subgroup, channels in hotel_data.items():
            print(f"      {subgroup}: {len(channels)} 个")
    print(f"  - 总计: {len(valid_channels) + total_hotel} 个源")

if __name__ == "__main__":
    asyncio.run(main())
