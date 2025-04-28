#!/usr/bin/env python3
"""
merge_av.py —— 强化鲁棒性的 FFmpeg 合并脚本
功能：
  - 交互 & CLI 模式二选一
  - 路径与扩展名校验
  - 磁盘空间检测
  - 超时执行与临时文件清理
  - 优雅处理所有异常与用户中断
"""

import shutil
import subprocess
import argparse
import sys
import logging
import atexit
import tempfile
import os
import signal
from pathlib import Path

# 支持的媒体扩展名
VIDEO_EXT = {".mp4", ".mov", ".mkv", ".avi", ".flv"}
AUDIO_EXT = {".mp3", ".aac", ".wav", ".flac", ".ogg"}

# 临时文件列表，用于退出时清理
_temp_files = []

def setup_logging(verbose: bool):
    level = logging.DEBUG if verbose else logging.INFO
    fmt = "%(asctime)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=level, format=fmt)

def check_ffmpeg():
    if not shutil.which("ffmpeg"):
        logging.error("未检测到 ffmpeg，请先安装并配置到 PATH。")
        sys.exit(1)

def validate_media_file(path: Path, allowed_exts: set, role: str):
    if not path.is_file():
        raise FileNotFoundError(f"{role} 文件不存在: {path}")
    if path.suffix.lower() not in allowed_exts:
        raise ValueError(f"{role} 扩展名不受支持: {path.suffix}")

def ensure_disk_space(target_dir: Path, required_bytes: int = 100 * 1024 * 1024):
    """检查目标目录是否至少有 required_bytes 可用空间，默认 100MB"""
    stat = shutil.disk_usage(str(target_dir))
    if stat.free < required_bytes:
        raise OSError(f"目标目录可用空间不足: {stat.free // (1024*1024)}MB")

def build_output_path(input_video: Path, output: str) -> Path:
    path = Path(output)
    if not path.parent or path.parent == Path(""):
        return input_video.resolve().parent / path.name
    return path

def cleanup_temp_files():
    for f in _temp_files:
        try:
            if Path(f).exists():
                Path(f).unlink()
                logging.debug(f"清理临时文件: {f}")
        except Exception:
            pass

# 注册退出清理
atexit.register(cleanup_temp_files)

def merge_av(input_video: Path, input_audio: Path, output_video: Path,
             overwrite: bool, verbose: bool, timeout: int):
    # 先校验
    validate_media_file(input_video, VIDEO_EXT, "视频")
    validate_media_file(input_audio, AUDIO_EXT, "音频")
    out_dir = output_video.parent
    out_dir.mkdir(parents=True, exist_ok=True)

    ensure_disk_space(out_dir)

    # 构造命令
    ff_cmd = [
        "ffmpeg",
        "-y" if overwrite else "-n",
        "-i", str(input_video),
        "-i", str(input_audio),
        "-c:v", "copy", "-c:a", "aac",
        "-map", "0:v:0", "-map", "1:a:0",
        "-shortest",
        str(output_video)
    ]
    logging.info(f"执行: {' '.join(ff_cmd)}")

    # 启动 FFmpeg 子进程
    proc = subprocess.Popen(
        ff_cmd,
        stdout=None if verbose else subprocess.DEVNULL,
        stderr=None if verbose else subprocess.DEVNULL
    )
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.send_signal(signal.SIGINT)
        raise TimeoutError("FFmpeg 合并超时已中断")
    if proc.returncode != 0:
        raise subprocess.CalledProcessError(proc.returncode, ff_cmd)
    logging.info(f"合并成功 👉 {output_video}")

def interactive_mode(force: bool, verbose: bool, timeout: int):
    try:
        vid = Path(input("请输入原视频文件路径: ").strip())
        aud = Path(input("请输入原音频文件路径: ").strip())
        base, ext = vid.stem, vid.suffix
        default = f"{base}-已转换{ext}"
        out_name = input(f"输出文件名（默认 {default}）: ").strip() or default
        output_path = build_output_path(vid, out_name)
        merge_av(vid, aud, output_path, force, verbose, timeout)
    except (KeyboardInterrupt, EOFError):
        print("\n喵～中途退出，已清理~")
        sys.exit(0)
    except Exception as e:
        logging.error(f"操作失败: {e}")
        sys.exit(1)

def parse_args():
    p = argparse.ArgumentParser(description="鲁棒性 FFmpeg 合并脚本")
    g = p.add_mutually_exclusive_group()
    g.add_argument('-i', '--interactive', action='store_true', help='交互模式')
    p.add_argument('-f', '--force', action='store_true', help='覆盖已存在输出')
    p.add_argument('-v', '--verbose', action='store_true', help='详细日志')
    p.add_argument('-t', '--timeout', type=int, default=300,
                   help='超时秒数，默认 300s')
    p.add_argument('video', nargs='?', help='视频文件路径')
    p.add_argument('audio', nargs='?', help='音频文件路径')
    p.add_argument('output', nargs='?', help='输出文件名')
    return p.parse_args()

def main():
    args = parse_args()
    setup_logging(args.verbose)
    check_ffmpeg()

    if args.interactive:
        interactive_mode(args.force, args.verbose, args.timeout)
    else:
        if not (args.video and args.audio and args.output):
            logging.error("参数不足，或与 -i 冲突。")
            sys.exit(1)
        vid = Path(args.video); aud = Path(args.audio)
        outp = build_output_path(vid, args.output)
        try:
            merge_av(vid, aud, outp, args.force, args.verbose, args.timeout)
        except Exception as e:
            logging.error(f"合并失败: {e}")
            sys.exit(1)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n喵～用户中断，已退出~")
        sys.exit(0)
