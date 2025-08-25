import logging
import os

def setup_logger(stage_name):
    """
    Sets up a logger for a specific ETL stage (extract, transform, load).
    Logs will be written both to console and a stage-specific log file.
    """
    log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")
    os.makedirs(log_dir, exist_ok=True)

    log_file = os.path.join(log_dir, f"{stage_name}.log")

    logger = logging.getLogger(stage_name)
    logger.setLevel(logging.INFO)

    
    if not logger.handlers:
      
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)

        
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger

def format_seconds(seconds: float) -> str:
    """Format seconds into h:m:s string."""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    seconds = int(seconds % 60)
    return f"{hours}h:{minutes}m:{seconds}s"