import os
import yaml
import logging.config

def setup_logging():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(current_dir, 'logging.yaml')

    if os.path.exists(config_path):
        with open(config_path, 'rt') as f:
            config = yaml.safe_load(f.read())
            logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=logging.WARNING)
        

def _model_dump(obj):
    if hasattr(obj, "model_dump"):      # Pydantic v2
        return obj.model_dump(exclude_none=True)
    elif hasattr(obj, "dict") and not isinstance(obj, dict):  # Pydantic v1
        return obj.dict(exclude_none=True)
    return obj  # plain dict or other serialisable type
        