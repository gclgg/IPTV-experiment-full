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
FAST_CHECK_TIMEOUT = 3
FFPROBE_TIMEOUT = 8
MIN_BITRATE = 200
MAX_RETRIES = 1
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
    return re.sub(r'[,\n\r]', ' ', group_name).strip()

def extract_logo_from_m3u(channel_name, m3u_file):
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
    channels_by_group = defaultdict(list)
    current_group = "未分组"
    m3u_file = filename.replace('.txt', '.m3u')
    
    with open(filename, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            if line.endswith('#genre#'):
                current_group = clean_group_name(line[:-7].strip())
                continue
            if ',' in line:
                parts = line.split(',', 1)
                channel_name = parts[0].strip()
                full_url = parts[1].strip()
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
    print(f"\n🏨 正在拉取酒店源: {HOTEL_SOURCE_URL}")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(HOTEL_SOURCE_URL, timeout=30) as resp:
                if resp.status != 200:
                    return {}
                content = await resp.text()
                hotel_by_subgroup = defaultdict(list)
                current_subgroup = None
                lines = content.strip().split('\n')
                for line in lines:
                    line = line.strip()
                    if not line:
                        continue
                    if line.endswith('#genre#'):
                        current_subgroup = line[:-7].strip()
                        continue
                    if ',' in line and current_subgroup:
                        parts = line.split(',', 1)
                        channel_name = parts[0].strip()
                        channel_url = parts[1].strip()
                        hotel_by_subgroup[current_subgroup].append({
                            'name': channel_name,
                            'url': channel_url
                        })
                total = sum(len(ch) for ch in hotel_by_subgroup.values())
                print(f"✅ 拉取成功，{len(hotel_by_subgroup)} 个子分组，{total} 个频道")
                return dict(hotel_by_subgroup)
    except Exception as e:
        print(f"❌ 拉取失败: {e}")
        return {}

async def fast_check(session, clean_url):
    try:
        async with session.head(clean_url, timeout=FAST_CHECK_TIMEOUT, allow_redirects=True) as resp:
            return resp.status in [200, 301, 302, 307, 308]
    except:
        return False

async def check_channel(session, channel):
    if channel.get('is_announcement'):
        return {
            'name': channel['name'],
            'group': channel['group'],
            'full_url': channel['full_url'],
            'logo': channel.get('logo', ''),
            'valid': True,
            'height': 1080,
            'quality_score': 10800000
        }
    
    clean_url = channel['clean_url']
    for attempt in range(MAX_RETRIES + 1):
        try:
            if not await fast_check(session, clean_url):
                if attempt < MAX_RETRIES:
                    await asyncio.sleep(0.5)
                    continue
                return None
            # 如果快速检测通过，直接返回一个默认有效的结果（跳过ffprobe）
            return {
                'name': channel['name'],
                'group': channel['group'],
                'full_url': channel['full_url'],
                'logo': channel.get('logo', ''),
                'valid': True,
                'height': 720,
                'quality_score': 7200000
            }
        except:
            if attempt == MAX_RETRIES:
                return None
            await asyncio.sleep(0.5)

async def main():
    import time
    start_time = time.time()
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # 拉取酒店源
    hotel_data = await fetch_hotel_source()
    
    # 解析本地源
    if not os.path.exists(INPUT_SOURCE):
        print(f"错误：文件 {INPUT_SOURCE} 不存在！")
        return
    
    channels_by_group = parse_txt_file(INPUT_SOURCE)
    
    # 分离公告和频道
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
    
    # 检测本地频道
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
    
    # 写入 M3U 文件
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write('#EXTM3U x-tvg-url="' + '","'.join(EPG_URLS) + '"\n')
        
        # 写入公告
        if announcement:
            f.write('\n# 分组：公告\n')
            tvg_id = str(abs(hash(f"更新日期 {current_time}")) % 10000)
            extinf = f'#EXTINF:-1 tvg-id="{tvg_id}" tvg-name="更新日期 {current_time}"'
            if announcement.get('logo'):
                extinf += f' tvg-logo="{announcement["logo"]}"'
            extinf += f' group-title="公告",更新日期 {current_time}'
            f.write(extinf + '\n')
            f.write(announcement['full_url'] + '\n')
        
        # 写入本地频道
        if valid_channels:
            f.write('\n# 分组：本地频道\n')
            for ch in valid_channels:
                tvg_id = str(abs(hash(ch['name'])) % 10000)
                extinf = f'#EXTINF:-1 tvg-id="{tvg_id}" tvg-name="{ch["name"]}"'
                if ch.get('logo'):
                    extinf += f' tvg-logo="{ch["logo"]}"'
                extinf += f' group-title="{ch["group"]}",{ch["name"]}'
                f.write(extinf + '\n')
                f.write(ch['full_url'] + '\n')
        
        # 写入酒店源
        if hotel_data:
            f.write(f'\n# 分组：{HOTEL_MAIN_GROUP}\n')
            for subgroup, channels in hotel_data.items():
                f.write(f'# 子分组：{subgroup}\n')
                for ch in channels:
                    tvg_id = str(abs(hash(ch['name'])) % 10000)
                    extinf = f'#EXTINF:-1 tvg-id="{tvg_id}" tvg-name="{ch["name"]}" group-title="{HOTEL_MAIN_GROUP}",{ch["name"]}'
                    f.write(extinf + '\n')
                    f.write(ch['url'] + '\n')
    
    elapsed = time.time() - start_time
    print(f"\n⏱️ 总耗时: {elapsed:.1f} 秒")
    print(f"🕐 更新时间: {current_time}")

if __name__ == "__main__":
    asyncio.run(main())
