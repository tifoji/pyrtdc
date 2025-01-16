import yaml
from pathlib import Path

def load_settings():
    """
    Load settings from config.yaml file.
    
    Returns:
        dict: Parsed configuration settings
        
    Raises:
        Exception: If config file is missing or malformed
    """
    try:
        config_path = Path(__file__).parent.parent.parent / 'config' / 'config.yaml'
        
        if not config_path.exists():
            raise Exception(f"Configuration file not found at: {config_path}")
            
        if not config_path.is_file():
            raise Exception(f"Path exists but is not a file: {config_path}")
            
        try:
            with open(config_path, 'r') as file:
                settings = yaml.safe_load(file)
                
            if not isinstance(settings, dict):
                raise Exception("Configuration file must contain a valid YAML dictionary")
                
            # Validate essential settings
            required_sections = ['rtd', 'logging', 'timing']
            missing_sections = [section for section in required_sections if section not in settings]
            
            if missing_sections:
                raise Exception(f"Missing required configuration sections: {', '.join(missing_sections)}")
                
            return settings
            
        except yaml.YAMLError as e:
            raise Exception(f"Error parsing YAML configuration: {str(e)}")
        except PermissionError as e:
            raise Exception(f"Permission denied accessing configuration file: {str(e)}")
            
    except Exception as e:
        print(f"Critical Error: {str(e)}")  # Fallback since logger might not be available
        raise

# Global settings object
SETTINGS = load_settings()