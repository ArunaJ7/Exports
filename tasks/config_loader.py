# config_loader.py (Configuration Loader - Singleton)
import threading
import configparser
from pathlib import Path
import logging
import platform

logger = logging.getLogger('appLogger')

class ConfigLoaderSingleton:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(ConfigLoaderSingleton, cls).__new__(cls)
                    cls._instance._load_config()
        return cls._instance

    def _load_config(self):
        try:
            project_root = Path(__file__).resolve().parents[1]
            config_path = project_root / 'config' / 'core_config.ini'
            if not config_path.exists():
                raise FileNotFoundError(f"Configuration file not found: {config_path}")

            self.config = configparser.ConfigParser()
            self.config.read(str(config_path))

            if 'environment' not in self.config or 'current' not in self.config['environment']:
                raise KeyError("Missing 'environment' section or 'current' key in config file.")

            self.environment = self.config['environment']['current'].lower()

        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            self.config = None
            self.environment = None

    def get_template_task_ids(self):
        if self.config is None:
            return []

        section = f'template_task_id_{self.environment}'
        if section not in self.config:
            logger.error(f"Missing section [{section}] in config file.")
            return []

        return [int(key) for key in self.config[section].keys() if key.isdigit()]


    def get_export_path(self):
        """Get export path from config based on environment and operating system"""
        if not self.config or not self.environment:
            raise ValueError("Export path configuration missing or environment not set")


        section = f'EXCEL_EXPORT_PATH_{self.environment}'
        if section not in self.config:
            raise ValueError(f"Missing section [{section}] in config file")
        
        system = platform.system()
        try:
            if system == 'Windows':
                return Path(self.config[section]['WIN_EXPORT_PATH'])
            elif system == 'Linux':
                return Path(self.config[section]['LIN_EXPORT_PATH'])
            else:
                raise OSError(f"Unsupported operating system: {system}")
        except KeyError as e:
            raise ValueError(f"Missing export path configuration for {system}") from e