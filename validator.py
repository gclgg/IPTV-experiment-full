import subprocess
import json
import asyncio
import aiohttp
import re
import os
import time

# --- 优化后的配置参数 ---
CONCURRENT_CHECKS = 20          # 提高并发数
FAST_CHECK_TIMEOUT = 5          # 快速 HEAD 检查超时（秒）
FFPROBE_TIMEOUT = 15             # ffprobe 超时时间（秒）
MIN_BITRATE = 500                # 最小码率要求 (kbps)
OUTPUT_FILE = "live.m3u"         # 最终生成的有效源文件
INPUT_SOURCE = "live.txt"        # 原始源列表文件
# ------------------------------------

def parse_txt_file(filename):
    """解析直播源 TXT 文件，返回结构化的数据"""
    channels_by_group = {}
    current_group = "未分组"
    
    with open(filename, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            
            if line.endswith('#genre#'):
                group_name = line[:-7].strip()
                current_group = group_name
                if current_group not in channels_by_group:
                    channels_by_group[current_group] = []
                continue
            
            if ',' in line:
                parts = line.split(',', 1)
                channel_name = parts[0].strip()
                url_part = parts[1].strip()
                
                url_match = re.search(r'(https?|rtsp)://[^\s,$]+', url_part)
                if url_match:
                    url = url_match.group(0)
                    if current_group not in channels_by_group:
                        channels_by_group[current_group] = []
                    
                    channels_by_group[current_group].append({
                        'name': channel_name,
                        'url': url,
                        'line_num': line_num
                    })
    
    return channels_by_group

async def fast_check(session, url):
    """快速 HEAD 检查，判断 URL 是否可达"""
    try:
        async with session.head(url, timeout=FAST_CHECK_TIMEOUT, allow_redirects=True) as resp:
            if resp.status in [200, 301, 302, 307, 308]:
                return True, resp.status
            else:
                return False, f"HTTP {resp.status}"
    except asyncio.TimeoutError:
        return False, "HEAD timeout"
    except Exception as e:
        return False, str(e)[:50]

async def ffprobe_check(url):
    """使用 ffprobe 详细检测流信息"""
    cmd = [
        'ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_streams',
        '-rw_timeout', f'{FFPROBE_TIMEOUT * 1000000}',
        '-user_agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        '-i', url
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
    url = channel['url']
    
    # 第一阶段：快速 HEAD 检查
    head_ok, head_result = await fast_check(session, url)
    if not head_ok:
        return {
            'group': channel['group'],
            'name': channel['name'],
            'url': url,
            'valid': False,
            'reason': f"HEAD failed: {head_result}"
        }
    
    # 第二阶段：ffprobe 详细检测
    probe_result = await ffprobe_check(url)
    
    return {
        'group': channel['group'],
        'name': channel['name'],
        'url': url,
        'valid': probe_result.get('valid', False),
        'resolution': probe_result.get('resolution', 'unknown'),
        'height': probe_result.get('height', 0),
        'reason': probe_result.get('reason', 'unknown') if not probe_result.get('valid') else None
    }

async def main():
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
                'url': channel['url']
            })

    # 3. 并发检测
    print(f"\n开始两阶段检测 {len(all_channels)} 个源（并发 {CONCURRENT_CHECKS}）...")
    
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(ssl=False),  # 禁用 SSL 验证加快速度
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

    # 5. 按分组生成 M3U
    valid_by_group = {}
    for ch in valid_channels:
        group = ch['group']
        if group not in valid_by_group:
            valid_by_group[group] = []
        valid_by_group[group].append(ch)

    # 按分辨率排序
    for group in valid_by_group:
        valid_by_group[group].sort(key=lambda x: x.get('height', 0), reverse=True)

    # 写入 M3U 文件
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write("#EXTM3U\n")
        
        for group in channels_by_group.keys():
            if group in valid_by_group and valid_by_group[group]:
                f.write(f"\n# 分组：{group}\n")
                for ch in valid_by_group[group]:
                    f.write(f'#EXTINF:-1 group-title="{group}" tvg-name="{ch["name"]}",{ch["name"]}\n')
                    f.write(f"{ch['url']}\n")

    print(f"\n已生成 {OUTPUT_FILE}，包含 {len(valid_channels)} 个有效源")

    # 写入无效源日志
    with open("invalid_sources.log", 'w', encoding='utf-8') as f:
        for ch in invalid_channels[:500]:  # 只记录前500个避免文件太大
            f.write(f"{ch['group']}\t{ch['name']}\t{ch['url']}\t{ch['reason']}\n")

if __name__ == "__main__":
    asyncio.run(main())
