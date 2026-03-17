import subprocess
import json
import asyncio
import aiohttp
import re
import os

# --- 配置参数 ---
CONCURRENT_CHECKS = 5      # 并发数（降低以防被 ban）
CHECK_TIMEOUT = 30         # 超时时间（秒）
MIN_BITRATE = 500          # 最小码率要求 (kbps)
OUTPUT_FILE = "live.m3u"   # 最终生成的有效源文件
INPUT_SOURCE = "live.txt"  # 原始源列表文件
# ------------------------------------

def extract_urls_from_file(filename):
    """从文件中提取所有 URL（支持频道名,URL 格式）"""
    urls = []
    url_pattern = re.compile(r'https?://[^\s,"]+')  # 匹配 http 或 https 链接，直到遇到空格、逗号或引号
    
    with open(filename, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line or line.startswith('#'):
                continue  # 跳过空行和注释行
            match = url_pattern.search(line)
            if match:
                urls.append(match.group(0))
            else:
                print(f"第 {line_num} 行未找到 URL: {line[:50]}...")
    
    return urls

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
    # 1. 提取 URL
    if not os.path.exists(INPUT_SOURCE):
        print(f"错误：文件 {INPUT_SOURCE} 不存在！")
        return
    
    urls = extract_urls_from_file(INPUT_SOURCE)
    print(f"从 {INPUT_SOURCE} 中共提取到 {len(urls)} 个 URL")
    
    if not urls:
        print("没有找到任何 URL，请检查文件格式。")
        return

    # 2. 并发检测
    print(f"开始检测 {len(urls)} 个源（并发 {CONCURRENT_CHECKS}）...")
    async with aiohttp.ClientSession() as session:
        semaphore = asyncio.Semaphore(CONCURRENT_CHECKS)
        
        async def bounded_check(url):
            async with semaphore:
                return await check_stream(session, url)
        
        tasks = [bounded_check(url) for url in urls]
        results = await asyncio.gather(*tasks)
    
    valid_streams = [r for r in results if r['valid']]
    invalid_streams = [r for r in results if not r['valid']]
    
    print(f"检测完成。有效源: {len(valid_streams)}，无效源: {len(invalid_streams)}")
    
    # 3. 输出部分失败原因以便调试
    if invalid_streams:
        print("前10个失败原因示例：")
        for i, inv in enumerate(invalid_streams[:10]):
            short_url = inv['url'][:60] + ('...' if len(inv['url']) > 60 else '')
            print(f"  {short_url} : {inv.get('reason', 'unknown')}")
    
    # 4. 按分辨率排序并写入文件
    valid_streams.sort(key=lambda x: x.get('height', 0), reverse=True)
    
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write("#EXTM3U\n")
        for stream in valid_streams:
            f.write(f"# 分辨率: {stream['resolution']}\n")
            f.write(f"{stream['url']}\n")
    
    print(f"已生成 {OUTPUT_FILE}，包含 {len(valid_streams)} 个有效源")
    
    with open("invalid_sources.log", 'w', encoding='utf-8') as f:
        for stream in invalid_streams:
            f.write(f"{stream['url']}\t{stream.get('reason', 'unknown')}\n")

if __name__ == "__main__":
    asyncio.run(main())
