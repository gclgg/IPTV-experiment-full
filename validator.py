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
CONCURRENT_CHECKS = 30          # 提高并发
FAST_CHECK_TIMEOUT = 3          # 快速检查
FFPROBE_TIMEOUT = 8             # 减少检测时间
MIN_BITRATE = 200               # 降低要求
MAX_RETRIES = 1                 # 减少重试
OUTPUT_FILE = "live.m3u"
INPUT_SOURCE = "live.txt"

# 酒店源配置（不检测）
HOTEL_SOURCE_URL = "https://raw.githubusercontent.com/gclgg/zubo/main/itvlist.txt"
HOTEL_MAIN_GROUP = "酒店源"  # 酒店源的主分组名称

# User-Agent 池
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
]

# EPG 源
EPG_URLS = [
    "http://epg.112114.xyz/pp.xml",
    "https://epg.112114.free.hr/pp.xml",
    "https://epg.112114.eu.org/pp.xml",
    "https://epg.v1.mk/fy.xml",
    "https://epg.v1.mk/fy.xml.gz",
    "http://epg.51zmt.top:8000/e.xml",
    "http://epg.51zmt.top:8000/e.xml.gz",
    "http://epg.aptvapp.com/xml",
    "https://epg.pw/xmltv/epg_CN.xml",
    "https://epg.pw/xmltv/epg_HK.xml",
    "https://epg.pw/xmltv/epg_TW.xml"
]

def clean_group_name(group_name):
    """清理分组名称"""
    return re.sub(r'[,\n\r]', ' ', group_name).strip()

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
    """
    拉取酒店源 itvlist.txt，不检测，直接解析为多级分组结构
    返回格式：{
        '央视频道': [{'name': 'CCTV1', 'url': '...'}, ...],
        '卫视频道': [...],
        ...
    }
    """
    print(f"\n🏨 正在拉取酒店源（不检测，直接合并）: {HOTEL_SOURCE_URL}")
    
    try:
        async with aiohttp.ClientSession(
            headers={'User-Agent': random.choice(USER_AGENTS)}
        ) as session:
            async with session.get(HOTEL_SOURCE_URL, timeout=30) as resp:
                if resp.status != 200:
                    print(f"❌ 拉取失败: HTTP {resp.status}")
                    return {}
                
                content = await resp.text()
                
                # 解析 TXT 格式，保持分组结构
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
                            'url': channel_url  # 保持原始URL，不做任何修改
                        })
                
                # 统计
                total_channels = sum(len(ch) for ch in hotel_by_subgroup.values())
                print(f"✅ 拉取成功，共 {len(hotel_by_subgroup)} 个子分组，{total_channels} 个频道")
                for subgroup, channels in hotel_by_subgroup.items():
                    print(f"   - {subgroup}: {len(channels)} 个频道")
                
                return dict(hotel_by_subgroup)
                
    except Exception as e:
        print(f"❌ 拉取失败: {str(e)}")
        return {}

async def fast_check(session, clean_url):
    """快速 HEAD 检查"""
    try:
        async with session.head(clean_url, timeout=FAST_CHECK_TIMEOUT, allow_redirects=True) as resp:
            if resp.status in [200, 301, 302, 307, 308]:
                return True
            return False
    except:
        return False

async def ffprobe_check(clean_url):
    """使用 ffprobe 检测流信息"""
    cmd = [
        'ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_streams',
        '-rw_timeout', f'{FFPROBE_TIMEOUT * 1000000}',
        '-user_agent', random.choice(USER_AGENTS),
        '-i', clean_url
    ]
    
    try:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=FFPROBE_TIMEOUT)
        
        if process.returncode != 0:
            return None
        
        data = json.loads(stdout)
        video_stream = next((s for s in data.get('streams', []) if s.get('codec_type') == 'video'), None)
        
        if video_stream:
            height = video_stream.get('height', 0)
            bitrate = video_stream.get('bit_rate', 0)
            
            if bitrate and int(bitrate) / 1000 < MIN_BITRATE:
                return None
            
            quality_score = (height * 10000) + (int(bitrate) / 1000 if bitrate else 0)
            
            return {
                'height': height,
                'quality_score': quality_score
            }
        return None
    except:
        return None

async def check_channel(session, channel):
    """检测单个本地频道"""
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
            
            probe_result = await ffprobe_check(clean_url)
            if not probe_result:
                if attempt < MAX_RETRIES:
                    await asyncio.sleep(0.5)
                    continue
                return None
            
            return {
                'name': channel['name'],
                'group': channel['group'],
                'full_url': channel['full_url'],
                'logo': channel.get('logo', ''),
                'valid': True,
                'height': probe_result['height'],
                'quality_score': probe_result['quality_score']
            }
        except:
            if attempt == MAX_RETRIES:
                return None
            await asyncio.sleep(0.5)

async def main():
    # 在 validator.py 的 main() 函数最开头添加
import subprocess
try:
    result = subprocess.run(['ffprobe', '-version'], capture_output=True, text=True, timeout=10)
    print("ffprobe 可用:", result.returncode == 0)
except Exception as e:
    print("ffprobe 调用失败:", e)
    import time
    start_time = time.time()
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # 1. 先拉取酒店源（不检测，快速完成）
    hotel_data = await fetch_hotel_source()
    
    # 2. 解析本地 live.txt
    if not os.path.exists(INPUT_SOURCE):
        print(f"错误：文件 {INPUT_SOURCE} 不存在！")
        return
    
    channels_by_group = parse_txt_file(INPUT_SOURCE)
    
    # 3. 分离公告和需要检测的频道
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
    
    # 4. 检测本地频道（只检测这一部分）
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
    
    # 5. 统计本地频道结果
    valid_channels = [r for r in results if r]
    invalid_count = len(channels_to_check) - len(valid_channels)
    
    print(f"\n✅ 本地频道检测完成！有效: {len(valid_channels)}，失效: {invalid_count}")
    print(f"有效比例: {len(valid_channels)/len(channels_to_check)*100:.1f}%")
    
    # 6. 按频道名分组本地有效源
    valid_by_name = defaultdict(list)
    for ch in valid_channels:
        valid_by_name[ch['name']].append(ch)
    
    # 7. 写入最终的 M3U 文件
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        # 写入 EPG 信息行
        f.write('#EXTM3U x-tvg-url="' + '","'.join(EPG_URLS) + '"\n')
        
        # === 第一部分：公告 ===
        if announcement:
            f.write('\n# 分组：公告\n')
            tvg_id = str(abs(hash(f"更新日期 {current_time}")) % 10000)
            extinf = f'#EXTINF:-1 tvg-id="{tvg_id}" tvg-name="更新日期 {current_time}"'
            if announcement.get('logo'):
                extinf += f' tvg-logo="{announcement["logo"]}"'
            extinf += f' group-title="公告",更新日期 {current_time}'
            f.write(extinf + '\n')
            f.write(announcement['full_url'] + '\n')
        
        # === 第二部分：本地有效频道（保持原分组结构） ===
        local_by_group = defaultdict(list)
        for name, sources in valid_by_name.items():
            # 按质量排序
            sources.sort(key=lambda x: -x['quality_score'])
            for idx, source in enumerate(sources, 1):
                clean_base = re.sub(r'\$.*$', '', source['full_url'])
                numbered_url = f"{clean_base}『线路{idx}』"
                local_by_group[source['group']].append({
                    'name': name,
                    'url': numbered_url,
                    'logo': source.get('logo', ''),
                    'height': source['height']
                })
        
        # 按原分组顺序写入本地频道
        for group in channels_by_group:
            if group != '公告' and group in local_by_group:
                # 组内按分辨率排序
                local_by_group[group].sort(key=lambda x: -x['height'])
                f.write(f'\n# 分组：{group}\n')
                for ch in local_by_group[group]:
                    tvg_id = str(abs(hash(ch['name'])) % 10000)
                    extinf = f'#EXTINF:-1 tvg-id="{tvg_id}" tvg-name="{ch["name"]}"'
                    if ch['logo']:
                        extinf += f' tvg-logo="{ch["logo"]}"'
                    extinf += f' group-title="{group}",{ch["name"]}'
                    f.write(extinf + '\n')
                    f.write(ch['url'] + '\n')
        
        # === 第三部分：酒店源（作为新的大分组，不检测直接合并） ===
        if hotel_data:
            f.write(f'\n# 分组：{HOTEL_MAIN_GROUP}\n')
            
            # 按子分组顺序写入酒店源
            for subgroup, channels in hotel_data.items():
                if channels:
                    # 在酒店源主分组下，用注释标记子分组
                    f.write(f'\n# 子分组：{subgroup}\n')
                    
                    for ch in channels:
                        tvg_id = str(abs(hash(ch['name'])) % 10000)
                        # group-title 仍然是主分组名
                        extinf = f'#EXTINF:-1 tvg-id="{tvg_id}" tvg-name="{ch["name"]}" group-title="{HOTEL_MAIN_GROUP}",{ch["name"]}'
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
    import time
    asyncio.run(main())
