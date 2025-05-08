#!/usr/bin/env python3

import os
import sys
import time
import signal
import logging
import subprocess
from datetime import datetime, timezone
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename=os.getenv('EC2_LOG_PATH', '/var/log/options-collector/watchdog.log')
)
logger = logging.getLogger(__name__)

class Watchdog:
    def __init__(self):
        self.process = None
        self.running = True
        self.restart_count = 0
        self.max_restarts = 10  # Maximum number of restarts in a 5-minute window
        self.restart_window = 300  # 5 minutes in seconds
        self.restart_times = []
        self._setup_signal_handlers()

    def _setup_signal_handlers(self):
        """Setup handlers for graceful shutdown."""
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)

    def _handle_shutdown(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, stopping watchdog...")
        self.running = False
        if self.process:
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()

    def _cleanup_old_restarts(self):
        """Remove restart times older than the window."""
        now = time.time()
        self.restart_times = [t for t in self.restart_times if now - t < self.restart_window]

    def _should_restart(self) -> bool:
        """Check if we should allow another restart."""
        self._cleanup_old_restarts()
        return len(self.restart_times) < self.max_restarts

    def start_process(self):
        """Start the main process."""
        try:
            # Get the directory of the current script
            script_dir = Path(__file__).parent.absolute()
            ec2_run_path = script_dir / "ec2_run.py"
            
            # Ensure the script exists
            if not ec2_run_path.exists():
                logger.error(f"ec2_run.py not found at {ec2_run_path}")
                return False

            # Start the process
            self.process = subprocess.Popen(
                [sys.executable, str(ec2_run_path)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True
            )
            
            logger.info(f"Started ec2_run.py with PID {self.process.pid}")
            return True
            
        except Exception as e:
            logger.exception(f"Failed to start process: {e}")
            return False

    def monitor(self):
        """Main monitoring loop."""
        logger.info("Starting watchdog monitoring...")
        
        while self.running:
            if not self.process or self.process.poll() is not None:
                # Process has died
                if self.process:
                    exit_code = self.process.poll()
                    stdout, stderr = self.process.communicate()
                    logger.error(f"Process died with exit code {exit_code}")
                    if stdout:
                        logger.error(f"stdout: {stdout}")
                    if stderr:
                        logger.error(f"stderr: {stderr}")
                
                # Check if we should restart
                if self._should_restart():
                    logger.info("Attempting to restart process...")
                    if self.start_process():
                        self.restart_times.append(time.time())
                        self.restart_count += 1
                        logger.info(f"Process restarted (restart count: {self.restart_count})")
                    else:
                        logger.error("Failed to restart process")
                else:
                    logger.error("Too many restarts in short period, stopping watchdog")
                    self.running = False
                    break
                
                # Wait a bit before checking again
                time.sleep(5)
            else:
                # Process is running, wait a bit before checking again
                time.sleep(1)

        logger.info("Watchdog stopped")

def main():
    watchdog = Watchdog()
    watchdog.monitor()

if __name__ == "__main__":
    main() 