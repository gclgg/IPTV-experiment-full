import subprocess
import json
import asyncio
import aiohttp
import re
import os
import random
from collections import defaultdict
from datetime import datetime

# --- 优化后的配置参数 ---
CONCURRENT_CHECKS = 10          # 降低并发，避免被屏蔽
FAST_CHECK_TIMEOUT = 8          # 快速 HEAD 检查超时（秒）
FFPROBE_TIMEOUT = 25            # ffprobe 超时时间（秒）
MIN_BITRATE = 300               # 降低码率要求
MAX_RETRIES = 2                 # 失败重试次数
OUTPUT_FILE = "live.m3u"
INPUT_SOURCE = "live.txt"

# User-Agent 池
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (Linux; Android 13; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36'
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
    """解析直播源 TXT 文件"""
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
    """检测单个频道（带重试）"""
    if channel.get('is_announcement'):
        return {
            'name': channel['name'],
            'group': channel['group'],
            'full_url': channel['full_url'],
            'logo': channel['logo'],
            'valid': True,
            'height': 1080,
            'quality_score': 10800000
        }
    
    clean_url = channel['clean_url']
    
    for attempt in range(MAX_RETRIES + 1):
        try:
            if not await fast_check(session, clean_url):
                if attempt < MAX_RETRIES:
                    await asyncio.sleep(1)
                    continue
                return None
            
            probe_result = await ffprobe_check(clean_url)
            if not probe_result:
                if attempt < MAX_RETRIES:
                    await asyncio.sleep(1)
                    continue
                return None
            
            return {
                'name': channel['name'],
                'group': channel['group'],
                'full_url': channel['full_url'],
                'logo': channel['logo'],
                'valid': True,
                'height': probe_result['height'],
                'quality_score': probe_result['quality_score']
            }
        except:
            if attempt == MAX_RETRIES:
                return None
            await asyncio.sleep(1)

async def main():
    start_time = time.time()
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    if not os.path.exists(INPUT_SOURCE):
        print(f"错误：文件 {INPUT_SOURCE} 不存在！")
        return
    
    channels_by_group = parse_txt_file(INPUT_SOURCE)
    
    # 分离公告和普通频道
    announcement = None
    normal_channels = []
    
    for group, channels in channels_by_group.items():
        for channel in channels:
            if group == '公告' and '更新日期' in channel['name']:
                announcement = channel
            elif group != '公告':
                normal_channels.append(channel)
    
    print(f"📢 公告: 1 条")
    print(f"📺 待检测频道: {len(normal_channels)} 个")
    
    # 检测普通频道
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(ssl=False),
        headers={'User-Agent': random.choice(USER_AGENTS)}
    ) as session:
        semaphore = asyncio.Semaphore(CONCURRENT_CHECKS)
        
        async def bounded_check(ch):
            async with semaphore:
                return await check_channel(session, ch)
        
        tasks = [bounded_check(ch) for ch in normal_channels]
        results = await asyncio.gather(*tasks)
    
    # 统计结果
    valid_channels = [r for r in results if r]
    invalid_count = len(normal_channels) - len(valid_channels)
    
    print(f"\n✅ 检测完成！有效源: {len(valid_channels)}，失效源: {invalid_count}")
    print(f"有效比例: {len(valid_channels)/len(normal_channels)*100:.1f}%")
    
    # 按频道名分组
    valid_by_name = defaultdict(list)
    for ch in valid_channels:
        valid_by_name[ch['name']].append(ch)
    
    # 按质量排序并写入文件
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write('#EXTM3U x-tvg-url="' + '","'.join(EPG_URLS) + '"\n')
        
        # 写入公告
        if announcement:
            f.write('\n# 分组：公告\n')
            tvg_id = str(abs(hash(f"更新日期 {current_time}")) % 10000)
            extinf = f'#EXTINF:-1 tvg-id="{tvg_id}" tvg-name="更新日期 {current_time}"'
            if announcement['logo']:
                extinf += f' tvg-logo="{announcement["logo"]}"'
            extinf += f' group-title="公告",更新日期 {current_time}'
            f.write(extinf + '\n')
            f.write(announcement['full_url'] + '\n')
        
        # 写入普通频道
        output_by_group = defaultdict(list)
        for name, sources in valid_by_name.items():
            sources.sort(key=lambda x: -x['quality_score'])
            for idx, source in enumerate(sources, 1):
                clean_base = re.sub(r'\$.*$', '', source['full_url'])
                numbered_url = f"{clean_base}『线路{idx}』"
                output_by_group[source['group']].append({
                    'name': name,
                    'url': numbered_url,
                    'logo': source['logo']
                })
        
        for group in channels_by_group:
            if group != '公告' and group in output_by_group:
                f.write(f'\n# 分组：{group}\n')
                for ch in output_by_group[group]:
                    tvg_id = str(abs(hash(ch['name'])) % 10000)
                    extinf = f'#EXTINF:-1 tvg-id="{tvg_id}" tvg-name="{ch["name"]}"'
                    if ch['logo']:
                        extinf += f' tvg-logo="{ch["logo"]}"'
                    extinf += f' group-title="{group}",{ch["name"]}'
                    f.write(extinf + '\n')
                    f.write(ch['url'] + '\n')
    
    elapsed = time.time() - start_time
    print(f"\n⏱️ 总耗时: {elapsed:.1f} 秒")
    print(f"🕐 更新时间: {current_time}")

if __name__ == "__main__":
    import time
    asyncio.run(main())
