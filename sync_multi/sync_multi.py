#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import time
import json
import logging
import fnmatch
import threading
import signal
import tempfile
import shutil
import platform
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from watchdog.events import FileSystemEventHandler

# —— Observer 选择 —— 喵♡～
if platform.system() == 'Darwin':
    from watchdog.observers.polling import PollingObserver as ObserverClass
else:
    from watchdog.observers import Observer as ObserverClass

# —— 配置 & 去抖 —— 喵♡～
CFG_PATH = Path("config.json")
DEBOUNCE = 1.0  # 秒

# —— 读取 & 校验配置 —— 喵♡～
if not CFG_PATH.exists():
    print(f"配置文件未找到：{CFG_PATH}，请创建喵♡～")
    sys.exit(1)

cfg = json.loads(CFG_PATH.read_text(encoding='utf-8'))
tasks_cfg = cfg.get("tasks", [])
if not isinstance(tasks_cfg, list) or not tasks_cfg:
    print("config.json 中 tasks 应为非空列表喵♡～")
    sys.exit(1)

# —— 信号 & 停止事件 —— 喵♡～
stop_event = threading.Event()
def _handle(signum, frame):
    stop_event.set()
    print("\n收到退出信号～准备停止喵♡～")
for sig in ("SIGINT", "SIGTERM"):
    if hasattr(signal, sig):
        signal.signal(getattr(signal, sig), _handle)

# —— 重试装饰器 —— 喵♡～
def retry(times=3, delay=0.5):
    def deco(fn):
        def wrapper(*a, **kw):
            for i in range(times):
                try: return fn(*a, **kw)
                except Exception as e:
                    if i < times-1: time.sleep(delay)
                    else: raise
        return wrapper
    return deco

# —— 同步任务 —— 喵♡～
class SyncTask:
    def __init__(self, cfg):
        self.name    = cfg.get("name", "sync_task")
        self.sources = [Path(p) for p in (cfg.get("sources") or [cfg.get("source")])]
        self.targets = [Path(p) for p in (cfg.get("targets") or [cfg.get("target")])]
        self.exclude = cfg.get("exclude", [])
        self.workers = cfg.get("workers", 4)
        self.logfile = Path(cfg.get("log", f"logs/{self.name}.log"))
        self._validate()
        self._setup_logger()
        self._lock = threading.Lock()
        self._timer = None

    def _validate(self):
        # 检查目录存在 & 权限喵♡～
        if not self.sources or not self.targets:
            raise ValueError(f"任务「{self.name}」需至少一个源和一个目标喵♡～")
        for s in self.sources:
            if not s.is_dir():
                raise ValueError(f"源目录不存在：{s} 喵♡～")
        for t in self.targets:
            t.mkdir(parents=True, exist_ok=True)
            test = t / f".sync_test_{int(time.time())}"
            try:
                test.write_text("ok")
                test.unlink()
            except Exception as e:
                raise ValueError(f"目标不可写：{t}；{e} 喵♡～")
        if len(self.sources)>1 and len(self.targets)>1 and len(self.sources)!=len(self.targets):
            raise ValueError("源与目标数量不匹配喵♡～")

    def _setup_logger(self):
        self.logfile.parent.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            fmt = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
            fh  = logging.FileHandler(self.logfile, encoding='utf-8')
            sh  = logging.StreamHandler()
            fh.setFormatter(fmt); sh.setFormatter(fmt)
            self.logger.addHandler(fh); self.logger.addHandler(sh)
        self.logger.info(f"启动「{self.name}」 Sources={self.sources} Targets={self.targets} Exclude={self.exclude} Workers={self.workers}")

    def should_exclude(self, path: Path, base: Path):
        rel = path.relative_to(base).as_posix()
        return any(fnmatch.fnmatch(rel, pat) for pat in self.exclude)

    def _pairs(self):
        if len(self.sources)==len(self.targets):
            return list(zip(self.sources, self.targets))
        if len(self.sources)==1:
            return [(self.sources[0], t) for t in self.targets]
        return [(s, self.targets[0]) for s in self.sources]

    @retry(times=3, delay=0.3)
    def _atomic_copy(self, src: Path, dst: Path):
        dst.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=dst.parent, delete=False) as tmp:
            tmp.write(src.read_bytes())
            tmp.flush()
        tmp_path = Path(tmp.name)
        tmp_path.replace(dst)

    @retry(times=3, delay=0.3)
    def _safe_delete(self, path: Path):
        if path.is_dir():
            path.rmdir()
        else:
            path.unlink()

    def gather(self):
        to_copy, to_delete = [], []
        for s_base, t_base in self._pairs():
            for src in s_base.rglob("*"):
                if src.is_file() and not self.should_exclude(src, s_base):
                    dst = t_base / src.relative_to(s_base)
                    if (not dst.exists()) or (src.stat().st_mtime > dst.stat().st_mtime):
                        to_copy.append((src, dst))
            for dst in t_base.rglob("*"):
                rel = dst.relative_to(t_base)
                src = s_base / rel
                if not src.exists():
                    to_delete.append(dst)
        return to_copy, to_delete

    def sync(self):
        if not self._lock.acquire(False):
            return
        try:
            copies, deletes = self.gather()
            with ThreadPoolExecutor(max_workers=self.workers) as pool:
                futures = [pool.submit(self._atomic_copy, s, d) for s, d in copies] + \
                          [pool.submit(self._safe_delete, p) for p in deletes]
                for _ in as_completed(futures): pass
            self.logger.info(f"「{self.name}」同步完毕：拷贝{len(copies)} 删除{len(deletes)} 喵♡～")
        except Exception as e:
            self.logger.error(f"同步出错：{e}", exc_info=True)
        finally:
            self._lock.release()

    class Handler(FileSystemEventHandler):
        def __init__(self, task):
            self.task = task
        def on_any_event(self, event):
            if self.task._timer and self.task._timer.is_alive():
                self.task._timer.cancel()
            self.task._timer = threading.Timer(DEBOUNCE, self.task.sync)
            self.task._timer.start()

    def start(self):
        self.sync()
        obs = ObserverClass()
        for s in self.sources:
            obs.schedule(self.Handler(self), str(s), recursive=True)
        obs.start()
        self.logger.info(f"监听已启动：{self.sources} → {self.targets} 喵♡～")
        return obs

# —— 启动所有任务 —— 喵♡～
observers = []
for tcfg in tasks_cfg:
    try:
        task = SyncTask(tcfg)
        observers.append(task.start())
    except Exception as e:
        print(f"任务「{tcfg.get('name','?')}」初始化失败：{e}")

try:
    while not stop_event.is_set():
        time.sleep(0.5)
finally:
    for o in observers:
        o.stop()
    print("所有任务已优雅停止喵♡～")
