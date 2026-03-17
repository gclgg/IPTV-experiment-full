import subprocess
import json
import asyncio
import aiohttp

# --- 配置参数 (根据你的实际文件名调整) ---
CONCURRENT_CHECKS = 20      # 同时检测的并发数
CHECK_TIMEOUT = 15          # 每个源的检测超时时间（秒）
MIN_BITRATE = 500           # 最小码率要求 (kbps)
OUTPUT_FILE = "live.m3u"    # 最终生成的文件名
INPUT_SOURCE = "live.txt"   # ⚠️ 重要：改成 main.py 生成的文件名
# ------------------------------------

async def check_stream(session, url):
    """使用 ffprobe 快速探测流信息，返回是否可用和分辨率"""
    cmd = [
        'ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_streams',
        '-rw_timeout', f'{CHECK_TIMEOUT * 1000000}',
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
            return {"url": url, "valid": False, "reason": "ffprobe failed"}
        
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
    # 注意：这里需要你的原始源列表文件 sources.txt
    # 我们稍后会处理这个文件
    with open(INPUT_SOURCE, 'r') as f:
        urls = [line.strip() for line in f if line.strip() and not line.startswith('#')]
    
    print(f"总共读取到 {len(urls)} 个源，开始检测有效性...")
    
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
    
    valid_streams.sort(key=lambda x: x.get('height', 0), reverse=True)
    
    with open(OUTPUT_FILE, 'w') as f:
        f.write("#EXTM3U\n")
        for stream in valid_streams:
            f.write(f"# 分辨率: {stream['resolution']}\n")
            f.write(f"{stream['url']}\n")
    
    print(f"已生成 {OUTPUT_FILE}，包含 {len(valid_streams)} 个有效源")
    
    with open("invalid_sources.log", 'w') as f:
        for stream in invalid_streams:
            f.write(f"{stream['url']}\t{stream.get('reason', 'unknown')}\n")

if __name__ == "__main__":
    asyncio.run(main())
