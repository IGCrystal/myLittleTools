#!/usr/bin/env python3
"""
merge_av.py —— 强化鲁棒性的 FFmpeg 合并脚本
功能：
  - 交互 & CLI 模式二选一
  - 路径与扩展名校验
  - 磁盘空间检测
  - 超时执行与临时文件清理
  - 默认输出 MP4（H.264 + AAC）
  - 根据输出容器智能选用音/视频编码器
  - 优雅处理所有异常与用户中断
"""

import shutil
import subprocess
import argparse
import sys
import logging
import atexit
import signal
from pathlib import Path

# 支持的媒体扩展名（包含 .webm）
VIDEO_EXT = {".mp4", ".mov", ".mkv", ".avi", ".flv", ".webm"}
AUDIO_EXT = {".mp3", ".aac", ".wav", ".flac", ".ogg"}

# 临时文件列表，用于退出时清理（目前暂无临时文件生成，可删或留）
_temp_files = []

def setup_logging(verbose: bool):
    """
    配置日志输出到标准输出，以避免干扰 input() 提示喵♡～
    """
    level = logging.DEBUG if verbose else logging.INFO
    fmt = "%(asctime)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=level, format=fmt, stream=sys.stdout)

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
    """
    强制输出为 .mp4：
      - 如果用户给的名字不是 .mp4 后缀，则改成 .mp4
      - 无路径时放到原视频目录
    """
    path = Path(output)
    # 强制为 .mp4 后缀
    if path.suffix.lower() != ".mp4":
        path = path.with_suffix(".mp4")
    # 只给了文件名，则放在原视频目录
    if path.parent == Path("."):
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

atexit.register(cleanup_temp_files)

def merge_av(input_video: Path, input_audio: Path, output_video: Path,
             overwrite: bool, verbose: bool, timeout: int):
    # 校验输入
    validate_media_file(input_video, VIDEO_EXT, "视频")
    validate_media_file(input_audio, AUDIO_EXT, "音频")
    out_dir = output_video.parent
    out_dir.mkdir(parents=True, exist_ok=True)
    ensure_disk_space(out_dir)

    # 根据输出容器选择视频/音频编码器
    suffix = output_video.suffix.lower()
    if suffix == ".webm":
        video_codec = "copy"
        audio_codec = "libvorbis"
    else:
        # 默认输出 MP4：H.264 + AAC
        video_codec = "libx264"
        audio_codec = "aac"

    # 构造 FFmpeg 命令
    ff_cmd = [
        "ffmpeg",
        "-y" if overwrite else "-n",
        "-i", str(input_video.resolve()),
        "-i", str(input_audio.resolve()),
        "-c:v", video_codec,
        "-c:a", audio_codec,
        "-map", "0:v:0", "-map", "1:a:0",
        "-shortest",
        str(output_video)
    ]
    logging.info(f"执行: {' '.join(ff_cmd)}")

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
    """
    交互模式：支持路径引号自动剥离，合并后继续，输入 'q' 退出。
    默认输出 MP4 喵♡～
    """
    print("进入交互模式（输入 q 或 quit 退出），喵~")
    try:
        while True:
            sys.stdout.flush()
            raw_vid = input("请输入原视频文件路径: ").strip()
            if raw_vid.lower() in ('q', 'quit'):
                break
            vid = Path(raw_vid.strip('"').strip("'"))

            sys.stdout.flush()
            raw_aud = input("请输入原音频文件路径: ").strip()
            if raw_aud.lower() in ('q', 'quit'):
                break
            aud = Path(raw_aud.strip('"').strip("'"))

            base = vid.stem
            default = f"{base}-已转换.mp4"
            sys.stdout.flush()
            raw_out = input(f"输出文件名（默认 {default}，输入 q 退出）: ").strip()
            if raw_out.lower() in ('q', 'quit'):
                break
            out_name = raw_out or default

            output_path = build_output_path(vid, out_name)
            try:
                merge_av(vid, aud, output_path, force, verbose, timeout)
                print(f"\n✅ 合并已完成 👉 {output_path}\n")
            except Exception as e:
                logging.error(f"❌ 本次合并失败: {e}")

            print("-" * 50)
            print("你可以继续下一组合并，或者输入 q 退出喵~")
            print("-" * 50)

    except KeyboardInterrupt:
        print("\n喵～用户中断，已退出~")
    finally:
        print("退出交互模式，拜拜喵！")
        sys.exit(0)

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
        vid = Path(args.video)
        aud = Path(args.audio)
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
