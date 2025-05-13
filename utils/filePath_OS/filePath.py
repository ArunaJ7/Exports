import configparser
import platform


def get_filePath(key):
    """
    Retrieves the file path for the given key based on the operating system.
    """
    # Initialize ConfigParser
    config = configparser.ConfigParser()

    # Load the .ini file (relative path)
    config_file_path = 'filePaths.ini'
    config.read(config_file_path)

    # Determine the operating system
    os_type = platform.system().lower()  # 'windows' or 'linux'

    # Map OS type to key suffix
    os_suffix = "WINDOWS" if os_type == 'windows' else "LINUX"

    # Retrieve the path
    if 'FILE_PATHS' in config:
        requested_key = f"{key}_{os_suffix}"
        path = config['FILE_PATHS'].get(requested_key, None)
        if path:
            return path
        else:
            return f"Key '{requested_key}' not found in the configuration file."
    else:
        return "FILE_PATHS section is missing in the configuration file."


# test function
if __name__ == "__main__":
    # Request paths dynamically
    log_path = get_filePath("LOG")
    print(f"Log Path: {log_path}")

    config_path = get_filePath("CONFIG")
    print(f"Config Path: {config_path}")
