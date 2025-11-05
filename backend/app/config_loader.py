# For reading config.yaml / .env

import os
import yaml

def load_config(path: str = None) -> dict:
    if path is None:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        backend_dir = os.path.dirname(current_dir)
        project_root = os.path.dirname(backend_dir)
        path = os.path.join(project_root, "backend", "config.yaml")
    
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = f.read()
        expanded = os.path.expandvars(raw)
        return yaml.safe_load(expanded) or {}
    except FileNotFoundError:
        print(f"Warning: Could not find config.yaml at {path}")
        return {}