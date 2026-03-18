import subprocess
import json
import asyncio
import aiohttp
import re
import os

# --- 配置参数 ---
CONCURRENT_CHECKS = 20          # 并发数
FAST_CHECK_TIMEOUT = 5          # 快速 HEAD 检查超时（秒）
FFPROBE_TIMEOUT = 15            # ffprobe 超时时间（秒）
MIN_BITRATE = 500               # 最小码率要求 (kbps)
OUTPUT_FILE = "live.m3u"        # 最终生成的有效源文件
INPUT_SOURCE = "live.txt"       # 原始源列表文件
# ------------------------------------

def parse_txt_file(filename):
    """
    解析直播源 TXT 文件，返回结构化的数据
    同时从原始 M3U 中提取 logo 信息（如果存在）
    """
    channels_by_group = {}
    current_group = "未分组"
    
    # 尝试读取同名的 .m3u 文件获取 logo 信息
    m3u_file = filename.replace('.txt', '.m3u')
    logo_cache = {}
    
    if os.path.exists(m3u_file):
        with open(m3u_file, 'r', encoding='utf-8') as f:
            for line in f:
                # 查找 EXTINF 行中的 tvg-logo
                if line.startswith('#EXTINF') and 'tvg-logo=' in line:
                    logo_match = re.search(r'tvg-logo="([^"]+)"', line)
                    name_match = re.search(r',([^,]+)$', line)
                    if logo_match and name_match:
                        logo_cache[name_match.group(1).strip()] = logo_match.group(1)
    
    with open(filename, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            
            # 检查是否是分组行（以 #genre# 结尾）
            if line.endswith('#genre#'):
                group_name = line[:-7].strip()
                current_group = group_name
                if current_group not in channels_by_group:
                    channels_by_group[current_group] = []
                continue
            
            # 处理频道行（格式：频道名,完整URL）
            if ',' in line:
                parts = line.split(',', 1)
                channel_name = parts[0].strip()
                full_url = parts[1].strip()  # 完整的URL，包含可能的$参数
                
                # 提取纯净的URL用于检测（去掉$后面的参数）
                clean_url = re.sub(r'\$.*$', '', full_url)
                
                # 获取 logo（从缓存或使用默认）
                logo_url = logo_cache.get(channel_name, '')
                
                if current_group not in channels_by_group:
                    channels_by_group[current_group] = []
                
                channels_by_group[current_group].append({
                    'name': channel_name,
                    'full_url': full_url,      # 带参数的完整URL（用于输出）
                    'clean_url': clean_url,    # 纯净URL（用于检测）
                    'logo': logo_url,          # logo URL
                    'line_num': line_num
                })
    
    return channels_by_group

async def fast_check(session, clean_url):
    """快速 HEAD 检查，判断 URL 是否可达"""
    try:
        async with session.head(clean_url, timeout=FAST_CHECK_TIMEOUT, allow_redirects=True) as resp:
            if resp.status in [200, 301, 302, 307, 308]:
                return True, resp.status
            else:
                return False, f"HTTP {resp.status}"
    except asyncio.TimeoutError:
        return False, "HEAD timeout"
    except Exception as e:
        return False, str(e)[:50]

async def ffprobe_check(clean_url):
    """使用 ffprobe 详细检测流信息"""
    cmd = [
        'ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_streams',
        '-rw_timeout', f'{FFPROBE_TIMEOUT * 1000000}',
        '-user_agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
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
            return {"valid": False, "reason": "ffprobe failed"}
        
        data = json.loads(stdout)
        
        video_stream = next((s for s in data.get('streams', []) if s.get('codec_type') == 'video'), None)
        
        if video_stream:
            width = video_stream.get('width', 0)
            height = video_stream.get('height', 0)
            bitrate = video_stream.get('bit_rate', 0)
            
            if bitrate and int(bitrate) / 1000 < MIN_BITRATE:
                return {"valid": False, "reason": f"bitrate too low: {int(bitrate)/1000:.0f}kbps"}
            
            resolution = f"{width}x{height}" if width and height else "unknown"
            return {
                "valid": True,
                "resolution": resolution,
                "height": height
            }
        else:
            return {"valid": False, "reason": "no video stream"}
            
    except asyncio.TimeoutError:
        return {"valid": False, "reason": "ffprobe timeout"}
    except Exception as e:
        return {"valid": False, "reason": str(e)[:50]}

async def check_channel(session, channel):
    """两阶段检测：先快速 HEAD，再 ffprobe"""
    clean_url = channel['clean_url']
    
    # 第一阶段：快速 HEAD 检查
    head_ok, head_result = await fast_check(session, clean_url)
    if not head_ok:
        return {
            'group': channel['group'],
            'name': channel['name'],
            'full_url': channel['full_url'],
            'logo': channel.get('logo', ''),
            'valid': False,
            'reason': f"HEAD failed: {head_result}"
        }
    
    # 第二阶段：ffprobe 详细检测
    probe_result = await ffprobe_check(clean_url)
    
    return {
        'group': channel['group'],
        'name': channel['name'],
        'full_url': channel['full_url'],
        'logo': channel.get('logo', ''),
        'valid': probe_result.get('valid', False),
        'resolution': probe_result.get('resolution', 'unknown'),
        'height': probe_result.get('height', 0),
        'reason': probe_result.get('reason', 'unknown') if not probe_result.get('valid') else None
    }

async def main():
    import time
    start_time = time.time()
    
    # 1. 解析文件
    if not os.path.exists(INPUT_SOURCE):
        print(f"错误：文件 {INPUT_SOURCE} 不存在！")
        return
    
    channels_by_group = parse_txt_file(INPUT_SOURCE)
    total_channels = sum(len(channels) for channels in channels_by_group.values())
    print(f"解析完成，共 {len(channels_by_group)} 个分组，{total_channels} 个频道")
    
    if total_channels == 0:
        print("没有找到任何频道，请检查文件格式。")
        return

    # 2. 收集所有频道
    all_channels = []
    for group, channels in channels_by_group.items():
        for channel in channels:
            all_channels.append({
                'group': group,
                'name': channel['name'],
                'full_url': channel['full_url'],
                'clean_url': channel['clean_url'],
                'logo': channel['logo']
            })

    # 3. 并发检测
    print(f"\n开始两阶段检测 {len(all_channels)} 个源（并发 {CONCURRENT_CHECKS}）...")
    
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(ssl=False),
        headers={'User-Agent': 'Mozilla/5.0'}
    ) as session:
        semaphore = asyncio.Semaphore(CONCURRENT_CHECKS)
        
        async def bounded_check(channel):
            async with semaphore:
                return await check_channel(session, channel)
        
        tasks = [bounded_check(ch) for ch in all_channels]
        results = await asyncio.gather(*tasks)

    # 4. 统计结果
    valid_channels = [r for r in results if r['valid']]
    invalid_channels = [r for r in results if not r['valid']]
    
    elapsed = time.time() - start_time
    print(f"\n检测完成！耗时: {elapsed:.1f} 秒")
    print(f"有效源: {len(valid_channels)}，无效源: {len(invalid_channels)}")

    # 5. 按分组生成 M3U（保持原始格式）
    valid_by_group = {}
    for ch in valid_channels:
        group = ch['group']
        if group not in valid_by_group:
            valid_by_group[group] = []
        valid_by_group[group].append(ch)

    # 按分辨率排序
    for group in valid_by_group:
        valid_by_group[group].sort(key=lambda x: x.get('height', 0), reverse=True)

    # 读取原始 M3U 文件获取 EPG 信息
    epg_urls = []
    original_m3u = INPUT_SOURCE.replace('.txt', '.m3u')
    if os.path.exists(original_m3u):
        with open(original_m3u, 'r', encoding='utf-8') as f:
            first_line = f.readline().strip()
            if first_line.startswith('#EXTM3U') and 'x-tvg-url=' in first_line:
                # 提取 EPG URLs
                epg_match = re.search(r'x-tvg-url="([^"]+)"', first_line)
                if epg_match:
                    epg_urls = epg_match.group(1).split('","')
    
    # 如果没有找到 EPG，使用默认列表
    if not epg_urls:
        epg_urls = [
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

    # 写入 M3U 文件（正确格式，保留所有原始信息）
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        # 写入 EPG 信息行
        epg_line = '#EXTM3U x-tvg-url="' + '","'.join(epg_urls) + '"'
        f.write(epg_line + '\n')
        
        # 按原始分组顺序写入频道
        for group in channels_by_group.keys():
            if group in valid_by_group and valid_by_group[group]:
                for ch in valid_by_group[group]:
                    # 生成 tvg-id（使用名称的哈希值）
                    tvg_id = str(abs(hash(ch['name'])) % 10000)
                    
                    # 构建完整的 EXTINF 行，包含 logo
                    extinf = f'#EXTINF:-1 tvg-id="{tvg_id}" tvg-name="{ch["name"]}"'
                    if ch['logo']:
                        extinf += f' tvg-logo="{ch["logo"]}"'
                    extinf += f' group-title="{group}",{ch["name"]}'
                    
                    f.write(extinf + '\n')
                    f.write(ch['full_url'] + '\n')  # 使用带参数的完整URL

    print(f"\n已生成 {OUTPUT_FILE}，包含 {len(valid_channels)} 个有效源")

    # 写入无效源日志
    with open("invalid_sources.log", 'w', encoding='utf-8') as f:
        for ch in invalid_channels[:500]:
            f.write(f"{ch['group']}\t{ch['name']}\t{ch['full_url']}\t{ch['reason']}\n")

if __name__ == "__main__":
    asyncio.run(main())
