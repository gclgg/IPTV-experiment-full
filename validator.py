import subprocess
import json
import asyncio
import aiohttp
import sys

# --- 配置参数 ---
CONCURRENT_CHECKS = 5      # 降低并发数
CHECK_TIMEOUT = 30         # 增加超时
MIN_BITRATE = 500
OUTPUT_FILE = "live.m3u"
INPUT_SOURCE = "live.txt"
# ------------------------------------

async def check_stream(session, url):
    """使用 ffprobe 快速探测流信息，返回是否可用和分辨率"""
    # 先尝试用 HEAD 请求检查可达性（可选）
    try:
        async with session.head(url, timeout=10, allow_redirects=True) as resp:
            if resp.status not in [200, 302]:
                return {"url": url, "valid": False, "reason": f"HTTP {resp.status}"}
    except Exception as e:
        return {"url": url, "valid": False, "reason": f"HEAD failed: {str(e)}"}

    cmd = [
        'ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_streams',
        '-rw_timeout', f'{CHECK_TIMEOUT * 1000000}',
        '-user_agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',  # 添加 UA
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
    # 读取源列表，过滤空行和注释行
    with open(INPUT_SOURCE, 'r') as f:
        lines = [line.strip() for line in f if line.strip()]
        # 简单过滤：只保留以 http 开头的行
        urls = [line for line in lines if line.startswith('http')]
    
    print(f"总共读取到 {len(urls)} 个 http 源（总行数 {len(lines)}）")
    
    if not urls:
        print("警告：没有找到任何以 http 开头的 URL，请检查 live.txt 格式")
        # 打印前5行原始内容以便调试
        with open(INPUT_SOURCE, 'r') as f:
            sample = [next(f) for _ in range(5)]
        print("live.txt 前5行：", sample)
        return

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
    
    # 打印部分失败原因
    if invalid_streams:
        print("前10个失败原因示例：")
        for i, inv in enumerate(invalid_streams[:10]):
            print(f"  {inv['url'][:80]}... : {inv.get('reason', 'unknown')}")
    
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
