import subprocess
import json
import asyncio
import aiohttp
import re
import os

# --- 配置参数 ---
CONCURRENT_CHECKS = 5      # 并发数
CHECK_TIMEOUT = 30         # 超时时间（秒）
MIN_BITRATE = 500          # 最小码率要求 (kbps)
OUTPUT_FILE = "live.m3u"   # 最终生成的有效源文件
INPUT_SOURCE = "live.txt"  # 原始源列表文件
# ------------------------------------

def parse_txt_file(filename):
    """
    解析直播源 TXT 文件，返回结构化的数据
    格式支持：
    - 分组行：分组名,#genre#
    - 频道行：频道名,URL
    """
    channels_by_group = {}
    current_group = "未分组"  # 默认分组
    
    with open(filename, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            
            # 检查是否是分组行（以 #genre# 结尾）
            if line.endswith('#genre#'):
                # 提取分组名（去掉最后的 ,#genre#）
                group_name = line[:-7].strip()
                current_group = group_name
                if current_group not in channels_by_group:
                    channels_by_group[current_group] = []
                continue
            
            # 处理频道行（格式：频道名,URL）
            if ',' in line:
                parts = line.split(',', 1)
                channel_name = parts[0].strip()
                # 提取 URL（可能包含额外参数如 $LR•IPV4『线路1』）
                url_part = parts[1].strip()
                
                # 使用正则提取第一个 http 或 rtsp 链接
                url_match = re.search(r'(https?|rtsp)://[^\s,$]+', url_part)
                if url_match:
                    url = url_match.group(0)
                    # 确保当前分组存在
                    if current_group not in channels_by_group:
                        channels_by_group[current_group] = []
                    
                    channels_by_group[current_group].append({
                        'name': channel_name,
                        'url': url,
                        'line_num': line_num,
                        'raw': line
                    })
                else:
                    print(f"第 {line_num} 行未找到有效 URL: {line[:50]}...")
    
    return channels_by_group

async def check_stream(session, url):
    """使用 ffprobe 探测流信息，返回是否可用和分辨率"""
    cmd = [
        'ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_streams',
        '-rw_timeout', f'{CHECK_TIMEOUT * 1000000}',
        '-user_agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        '-i', url
    ]
    
    try:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=CHECK_TIMEOUT)
        
        if process.returncode != 0:
            error_msg = stderr.decode()[:200] if stderr else "ffprobe failed"
            return {"url": url, "valid": False, "reason": f"ffprobe error: {error_msg}"}
        
        data = json.loads(stdout)
        
        video_stream = next((s for s in data.get('streams', []) if s.get('codec_type') == 'video'), None)
        
        if video_stream:
            width = video_stream.get('width', 0)
            height = video_stream.get('height', 0)
            bitrate = video_stream.get('bit_rate', 0)
            
            if bitrate and int(bitrate) / 1000 < MIN_BITRATE:
                return {"url": url, "valid": False, "reason": f"bitrate too low: {int(bitrate)/1000}kbps"}
            
            resolution = f"{width}x{height}" if width and height else "unknown"
            return {
                "url": url, 
                "valid": True, 
                "resolution": resolution,
                "height": height
            }
        else:
            return {"url": url, "valid": False, "reason": "no video stream"}
            
    except asyncio.TimeoutError:
        return {"url": url, "valid": False, "reason": "timeout"}
    except Exception as e:
        return {"url": url, "valid": False, "reason": str(e)}

async def main():
    # 1. 检查输入文件
    if not os.path.exists(INPUT_SOURCE):
        print(f"错误：文件 {INPUT_SOURCE} 不存在！")
        return
    
    # 2. 解析 TXT 文件，获取分组后的频道列表
    channels_by_group = parse_txt_file(INPUT_SOURCE)
    
    total_channels = sum(len(channels) for channels in channels_by_group.values())
    print(f"解析完成，共 {len(channels_by_group)} 个分组，{total_channels} 个频道")
    
    # 打印分组统计
    for group, channels in channels_by_group.items():
        print(f"  - {group}: {len(channels)} 个频道")
    
    if total_channels == 0:
        print("没有找到任何频道，请检查文件格式。")
        return

    # 3. 收集所有需要检测的 URL
    all_channels = []
    for group, channels in channels_by_group.items():
        for channel in channels:
            all_channels.append({
                'group': group,
                'name': channel['name'],
                'url': channel['url']
            })
    
    # 4. 并发检测所有 URL
    print(f"\n开始检测 {len(all_channels)} 个源（并发 {CONCURRENT_CHECKS}）...")
    async with aiohttp.ClientSession() as session:
        semaphore = asyncio.Semaphore(CONCURRENT_CHECKS)
        
        async def bounded_check(channel):
            async with semaphore:
                result = await check_stream(session, channel['url'])
                # 将频道信息合并到结果中
                result['group'] = channel['group']
                result['name'] = channel['name']
                return result
        
        tasks = [bounded_check(ch) for ch in all_channels]
        results = await asyncio.gather(*tasks)
    
    # 5. 按分组整理结果
    valid_by_group = {}
    invalid_by_group = {}
    
    for result in results:
        group = result['group']
        if result['valid']:
            if group not in valid_by_group:
                valid_by_group[group] = []
            valid_by_group[group].append(result)
        else:
            if group not in invalid_by_group:
                invalid_by_group[group] = []
            invalid_by_group[group].append(result)
    
    # 统计
    total_valid = sum(len(v) for v in valid_by_group.values())
    total_invalid = sum(len(v) for v in invalid_by_group.values())
    
    print(f"\n检测完成。有效源: {total_valid}，无效源: {total_invalid}")
    
    # 按分组打印统计
    print("\n各分组有效源统计：")
    for group in channels_by_group.keys():
        valid_count = len(valid_by_group.get(group, []))
        invalid_count = len(invalid_by_group.get(group, []))
        total = valid_count + invalid_count
        if total > 0:
            print(f"  {group}: {valid_count}/{total} 有效 ({valid_count/total*100:.1f}%)")
    
    # 6. 输出部分失败原因
    if total_invalid > 0:
        print("\n前10个失败原因示例：")
        fail_samples = []
        for group, invalids in invalid_by_group.items():
            for inv in invalids[:3]:  # 每个分组最多取3个
                fail_samples.append(inv)
                if len(fail_samples) >= 10:
                    break
            if len(fail_samples) >= 10:
                break
        
        for inv in fail_samples[:10]:
            short_url = inv['url'][:60] + ('...' if len(inv['url']) > 60 else '')
            print(f"  [{inv['group']}] {inv['name']}: {short_url} -> {inv.get('reason', 'unknown')}")
    
    # 7. 生成带分组的 M3U 文件
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write("#EXTM3U\n")
        
        # 按分组写入，保持分组顺序（按原文件中的顺序）
        for group in channels_by_group.keys():
            valid_channels = valid_by_group.get(group, [])
            if valid_channels:
                # 在每个分组前添加注释作为分组标记
                # 有些播放器支持 #EXTGRP 标签，但更通用的方式是在 EXTINF 中使用 group-title
                f.write(f"\n# 分组：{group}\n")
                
                # 对分组内的频道按分辨率排序
                valid_channels.sort(key=lambda x: x.get('height', 0), reverse=True)
                
                for channel in valid_channels:
                    # 写入频道信息行（使用 group-title 属性）
                    f.write(f'#EXTINF:-1 group-title="{group}" tvg-name="{channel["name"]}",{channel["name"]}\n')
                    # 写入 URL
                    f.write(f"{channel['url']}\n")
    
    print(f"\n已生成 {OUTPUT_FILE}，包含 {total_valid} 个有效源，分布在 {len(valid_by_group)} 个分组中")
    
    # 8. 生成无效源日志
    with open("invalid_sources.log", 'w', encoding='utf-8') as f:
        for group, invalids in invalid_by_group.items():
            f.write(f"\n# 分组：{group}\n")
            for inv in invalids:
                f.write(f"{inv['name']}\t{inv['url']}\t{inv.get('reason', 'unknown')}\n")

if __name__ == "__main__":
    asyncio.run(main())
