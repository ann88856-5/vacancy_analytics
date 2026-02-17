from abc import ABC, abstractmethod
import json
import os
from datetime import datetime
from typing import List, Dict, Any


class BaseParser(ABC):
    """Базовый класс для всех парсеров"""
    
    def __init__(self, source_name: str):
        self.source_name = source_name
        self.raw_data_dir = "data/raw"
        
        os.makedirs(self.raw_data_dir, exist_ok=True)
    
    @abstractmethod
    def parse(self, query: str, pages: int = 1) -> List[Dict[str, Any]]:
        pass
    
    def save_raw_data(self, data: List[Dict[str, Any]], filename: str = None):
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{self.source_name}_{timestamp}.json"
        
        filepath = os.path.join(self.raw_data_dir, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        print(f" Сохранено {len(data)} вакансий в {filepath}")
        return filepath
