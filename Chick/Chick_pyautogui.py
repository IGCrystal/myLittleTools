import pyautogui
import keyboard
import time
import threading
import sys
import logging
import psutil

pyautogui.FAILSAFE = True  # 启用安全保护

# 配置日志
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class Clicker:
    def __init__(self, min_interval=0.001, max_interval=1.0, default_interval=0.01):
        self.clicking_event = threading.Event()
        self.interval_lock = threading.Lock()
        self.min_interval = min_interval
        self.max_interval = max_interval
        self.active_interval = default_interval
        self.inactive_interval = 0.5  # 非活动检测间隔

    def toggle_clicking(self):
        """ 开启/暂停点击 """
        if self.clicking_event.is_set():
            self.clicking_event.clear()
            logging.info("🛑 点击已暂停")
        else:
            self.clicking_event.set()
            logging.info("▶️ 点击进行中")

    def adjust_interval(self, delta):
        """ 调整点击间隔 """
        with self.interval_lock:
            new_val = self.active_interval + delta
            self.active_interval = max(self.min_interval, min(new_val, self.max_interval))

            status = ""
            if self.active_interval == self.min_interval:
                status = " (极限速度)"
            elif self.active_interval == self.max_interval:
                status = " (最低速度)"
            logging.info(f"⏱️ 当前间隔：{self.active_interval:.3f}秒{status}")

    def increase_speed(self):
        """ 加快点击速度 """
        self.adjust_interval(-0.005)

    def decrease_speed(self):
        """ 减慢点击速度 """
        self.adjust_interval(0.005)

    def click_engine(self):
        """ 点击主循环 """
        while True:
            if self.clicking_event.is_set():
                try:
                    pyautogui.click()
                    time.sleep(self.active_interval)
                except pyautogui.FailSafeException:
                    logging.warning("❌ 安全保护触发！鼠标移至屏幕左上角")
                    self.clicking_event.clear()
                except Exception as e:
                    logging.error(f"⚠️ 点击异常：{e}")
                    self.clicking_event.clear()
            else:
                time.sleep(self.inactive_interval)

    def resource_monitor(self):
        """ 系统资源监控 """
        while True:
            if self.clicking_event.is_set():
                cpu = psutil.cpu_percent()
                mem = psutil.virtual_memory().percent
                logging.info(f"📊 系统负载 | CPU: {cpu:.1f}% | 内存: {mem:.1f}%")
            time.sleep(5)

    def graceful_exit(self):
        """ 退出程序 """
        logging.info("\n🛑 正在停止所有线程...")
        self.clicking_event.clear()
        time.sleep(0.2)  # 等待当前点击完成
        logging.info("✅ 资源已释放")
        sys.exit(0)

    def exit_handler(self):
        """ 监听 ESC 退出 """
        logging.info("⏎ 按 ESC 退出")
        keyboard.wait("esc")
        self.graceful_exit()

    def start(self):
        """ 启动所有线程 """
        logging.info("🔥 Egg, Inc. 专业版点击器")
        logging.info("==========================")
        logging.info("功能说明：")
        logging.info("- Ctrl+Shift+S : 启动/停止点击")
        logging.info("- Ctrl+↑       : 每次加速0.005秒")
        logging.info("- Ctrl+↓       : 每次减速0.005秒")
        logging.info("- ESC          : 安全退出程序")
        logging.info("==========================")

        # 绑定热键
        keyboard.add_hotkey("ctrl+shift+s", self.toggle_clicking)
        keyboard.add_hotkey("ctrl+up", self.increase_speed)
        keyboard.add_hotkey("ctrl+down", self.decrease_speed)

        # 启动点击线程
        click_thread = threading.Thread(target=self.click_engine, daemon=True)
        click_thread.start()

        # 启动资源监控线程
        monitor_thread = threading.Thread(target=self.resource_monitor, daemon=True)
        monitor_thread.start()

        # 启动退出监听线程
        exit_thread = threading.Thread(target=self.exit_handler, daemon=True)
        exit_thread.start()

        exit_thread.join()  # 阻塞主线程


if __name__ == "__main__":
    clicker = Clicker()
    clicker.start()
