#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import fnmatch
import hashlib
import json
import logging
import os
import platform
import random
import shutil
import signal
import sys
import tempfile
import threading
import time
from logging.handlers import RotatingFileHandler
from multiprocessing import Process
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver

# —— 猫娘尾巴 ——
CAT_TAILS = ["喵～", "喵♡～", "呜喵～", "噜～"]
def random_tail() -> str:
    return random.choice(CAT_TAILS)

# —— 8. 资源限制 ——
try:
    import resource
    resource.setrlimit(resource.RLIMIT_AS, (1 << 30, 1 << 30))
    resource.setrlimit(resource.RLIMIT_CPU, (3600, 3600))
except Exception:
    pass

CFG_PATH = Path("config.json")
DEBOUNCE = 1.0
HEARTBEAT_INTERVAL = 3600
RESTART_DELAY = 5

# —— 猫娘日志格式 + 轮转 ——
class CatFormatter(logging.Formatter):
    def format(self, record):
        ct = self.formatTime(record, "%Y-%m-%d %H:%M:%S")
        return f"{ct} | {record.levelname:^5} | {record.getMessage()} {random_tail()}"

def setup_logger(name: str, logfile: Path) -> logging.Logger:
    logfile.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if not logger.handlers:
        fmt = CatFormatter()
        fh = RotatingFileHandler(
            logfile, maxBytes=10*1024*1024, backupCount=5, encoding="utf-8"
        )
        fh.setFormatter(fmt)
        sh = logging.StreamHandler()
        sh.setFormatter(fmt)
        logger.addHandler(fh)
        logger.addHandler(sh)
    return logger

def retry(times=3, delay=0.5):
    def deco(fn):
        def wrapper(*a, **kw):
            for i in range(times):
                try:
                    return fn(*a, **kw)
                except Exception:
                    if i < times - 1:
                        time.sleep(delay)
                    else:
                        raise
        return wrapper
    return deco

def compute_hash(path: Path, algo="sha256", chunk_size=8192) -> str:
    h = hashlib.new(algo)
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()

class SyncTask:
    def __init__(self, cfg: dict):
        self.name    = cfg.get("name", "sync_task")
        srcs = cfg.get("sources") or [cfg.get("source")]
        tgts = cfg.get("targets") or [cfg.get("target")]
        self.sources = [Path(p) for p in srcs if p]
        self.targets = [Path(p) for p in tgts if p]
        self.exclude = cfg.get("exclude", [])
        self.workers = cfg.get("workers", 4)
        self.logfile = Path(cfg.get("log", f"logs/{self.name}.log"))

        # 同步控制
        self._lock            = threading.Lock()
        self._timer           = None
        self._pending         = False
        self._paths_lock      = threading.Lock()
        self._pending_paths   = set()
        self._counter_lock    = threading.Lock()
        self._copy_count      = 0
        self._delete_count    = 0

        self.logger = setup_logger(self.name, self.logfile)
        self._validate()
        self.logger.info(f"🟢 启动任务「{self.name}」")

    def _validate(self):
        if not (self.sources and self.targets):
            raise ValueError("需至少一个源和一个目标")
        for s in self.sources:
            if not s.is_dir():
                raise ValueError(f"源不存在：{s}")
        for t in self.targets:
            t.mkdir(parents=True, exist_ok=True)
            test = t / f".sync_test_{int(time.time())}"
            try:
                test.write_text("ok"); test.unlink()
            except Exception as e:
                raise ValueError(f"目标不可写：{t}；{e}")

    def _pairs(self):
        if len(self.sources) == len(self.targets):
            return list(zip(self.sources, self.targets))
        if len(self.sources) == 1:
            return [(self.sources[0], t) for t in self.targets]
        return [(s, self.targets[0]) for s in self.sources]

    def should_exclude(self, path: Path, base: Path) -> bool:
        rel = path.relative_to(base).as_posix()
        return any(fnmatch.fnmatch(rel, pat) for pat in self.exclude)

    def cleanup_tmp_files(self):
        for _, t_base in self._pairs():
            for tmp in t_base.rglob("*.sync_tmp*"):
                try:
                    tmp.unlink()
                    self.logger.info(f"🧹 清理临时文件：{tmp}")
                except: pass

    @retry(times=3, delay=0.3)
    def _atomic_copy(self, src: Path, dst: Path):
        if src.is_symlink():
            target = os.readlink(src)
            try: dst.unlink()
            except: pass
            os.symlink(target, dst)
            try: shutil.copystat(src, dst, follow_symlinks=False)
            except: pass
            return
        dst.parent.mkdir(parents=True, exist_ok=True)
        tmp = tempfile.NamedTemporaryFile(dir=dst.parent, delete=False)
        try:
            with src.open("rb") as fsrc, tmp:
                shutil.copyfileobj(fsrc, tmp); tmp.flush()
            Path(tmp.name).replace(dst)
            try: shutil.copystat(src, dst, follow_symlinks=False)
            except: pass
        finally:
            if tmp and Path(tmp.name).exists():
                try: Path(tmp.name).unlink()
                except: pass

    @retry(times=3, delay=0.3)
    def _safe_delete(self, path: Path):
        if path.is_dir():
            path.rmdir()
        else:
            path.unlink()

    def _wrapped_copy(self, src, dst, sem):
        try:
            self._atomic_copy(src, dst)
            with self._counter_lock:
                self._copy_count += 1
            self.logger.info(f"📄 复制: {src} → {dst}")
        finally:
            sem.release()

    def _wrapped_delete(self, path, sem):
        try:
            self._safe_delete(path)
            with self._counter_lock:
                self._delete_count += 1
            self.logger.info(f"🗑 删除: {path}")
        finally:
            sem.release()

    def sync(self):
        # 批量变动汇总
        with self._paths_lock:
            changed = list(self._pending_paths)
            self._pending_paths.clear()
        if changed:
            txt = "; ".join(str(p) for p in changed)
            self.logger.info(f"✨ 检测到变动 {len(changed)} 条: {txt}")

        if not self._lock.acquire(False):
            self._pending = True
            return

        with self._counter_lock:
            self._copy_count   = 0
            self._delete_count = 0

        start = time.time()
        self.logger.info("🕒 开始同步")
        sem = threading.Semaphore(self.workers * 2)
        try:
            with ThreadPoolExecutor(max_workers=self.workers) as pool:
                for s_base, t_base in self._pairs():
                    for src in s_base.rglob("*"):
                        try:
                            if (src.is_file()
                                and not self.should_exclude(src, s_base)):
                                dst = t_base / src.relative_to(s_base)
                                need = False
                                if not dst.exists():
                                    need = True
                                else:
                                    if src.stat().st_mtime > dst.stat().st_mtime:
                                        if compute_hash(src) != compute_hash(dst):
                                            need = True
                                if need:
                                    sem.acquire()
                                    pool.submit(
                                        self._wrapped_copy, src, dst, sem
                                    )
                        except: continue
                    for dst in t_base.rglob("*"):
                        try:
                            rel = dst.relative_to(t_base).as_posix()
                            if any(fnmatch.fnmatch(rel, pat)
                                   for pat in self.exclude):
                                continue
                            src = s_base / rel
                            if not src.exists():
                                sem.acquire()
                                pool.submit(
                                    self._wrapped_delete, dst, sem
                                )
                        except: continue
                pool.shutdown(wait=True)

            elapsed = time.time() - start
            self.logger.info(
                f"✅ 同步完成：复制 {self._copy_count} 个，"
                f"删除 {self._delete_count} 个，耗时 {elapsed:.2f}s"
            )
        except Exception as e:
            self.logger.error(f"同步出错：{e}", exc_info=True)
        finally:
            self._lock.release()
            if self._pending:
                self._pending = False
                self.sync()

    class Handler(FileSystemEventHandler):
        def __init__(self, task):
            self.task = task

        def on_any_event(self, event):
            with self.task._paths_lock:
                self.task._pending_paths.add(Path(event.src_path))
            self.task.sync()
            if self.task._timer and self.task._timer.is_alive():
                self.task._timer.cancel()
            self.task._timer = threading.Timer(
                DEBOUNCE, self.task.sync)
            self.task._timer.start()

    def _heartbeat_loop(self):
        while True:
            time.sleep(HEARTBEAT_INTERVAL)
            self.logger.info(f"🔄 心跳：任务「{self.name}」正常运行")

    def start(self):
        self.cleanup_tmp_files()
        self.sync()
        threading.Thread(
            target=self._heartbeat_loop, daemon=True
        ).start()
        obs_list = []
        for s in self.sources:
            ObsCls = PollingObserver if (
                platform.system() == "Darwin"
            ) else Observer
            obs = ObsCls()
            obs.schedule(self.Handler(self), str(s), recursive=True)
            obs.start()
            self.logger.info(f"👀 监听: {s}")
            obs_list.append(obs)
        return obs_list

# —— 动态重载配置 ——
tasks: list[SyncTask] = []
observers: list = []

class ConfigReloader(FileSystemEventHandler):
    def __init__(self):
        super().__init__()
        self._timer = None

    def on_modified(self, event):
        if Path(event.src_path).resolve() == CFG_PATH.resolve():
            if self._timer and self._timer.is_alive():
                self._timer.cancel()
            self._timer = threading.Timer(DEBOUNCE, reload_config)
            self._timer.start()

def reload_config():
    logging.info("🔄 配置变更，重新加载任务")
    for o in observers:
        o.stop()
    for o in observers:
        o.join()
    observers.clear()
    tasks.clear()
    try:
        cfg = json.loads(CFG_PATH.read_text(encoding="utf-8"))
        for tcfg in cfg.get("tasks", []):
            try:
                task = SyncTask(tcfg)
                tasks.append(task)
                for o in task.start():
                    observers.append(o)
            except Exception as e:
                logging.error(f"任务初始化失败：{e}")
    except Exception as e:
        logging.error(f"加载 config.json 失败：{e}")

def sync_worker():
    logger = logging.getLogger("sync_worker")
    try:
        cfg_obs = PollingObserver()
        cfg_obs.schedule(ConfigReloader(), str(CFG_PATH.parent),
                         recursive=False)
        cfg_obs.start()
        observers.append(cfg_obs)
        reload_config()
        while True:
            time.sleep(0.5)
    except KeyboardInterrupt:
        logger.info("子进程收到退出信号，优雅退出")
    finally:
        for o in observers:
            o.stop()
        for o in observers:
            o.join()

def supervise():
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setFormatter(CatFormatter())
    root_logger.addHandler(handler)
    try:
        while True:
            p = Process(target=sync_worker, name="sync_worker")
            p.start()
            p.join()
            root_logger.error(
                f"🚨 子进程退出(code={p.exitcode})，{RESTART_DELAY}s 后重启"
            )
            time.sleep(RESTART_DELAY)
    except KeyboardInterrupt:
        root_logger.info("父进程收到退出信号，优雅退出")
        sys.exit(0)

if __name__ == "__main__":
    supervise()
