"""
Парсер для HeadHunter API
Документация: https://github.com/hhru/api
"""

import requests
import time
import json
import os
import sys
from datetime import datetime
from typing import List, Dict, Any

# Добавляем корневую папку в путь
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from parsers.base_parser import BaseParser


class HHParser(BaseParser):
    """
    Парсер для сбора вакансий с HeadHunter через API
    """
    
    def __init__(self):
        super().__init__("hh")
        self.base_url = "https://api.hh.ru/vacancies"
        # Без User-Agent HH может заблокировать
        self.headers = {
            'User-Agent': 'HH-User-Agent/1.0 (my-app@example.com)'
        }
    
    def parse(self, query: str = "Python", pages: int = 5) -> List[Dict[str, Any]]:
        """
        Парсит вакансии с HeadHunter API
        
        Args:
            query: поисковый запрос (например, "Python")
            pages: количество страниц (на каждой до 100 вакансий)
        
        Returns:
            List[Dict] - список вакансий
        """
        print(f" Поиск вакансий по запросу: '{query}'")
        
        all_vacancies = []
        
        for page in range(pages):
            print(f" Загрузка страницы {page + 1}/{pages}...")
            
            params = {
                'text': query,
                'area': 1,  
                'per_page': 100,  
                'page': page
            }
            
            try:
                response = requests.get(
                    self.base_url,
                    params=params,
                    headers=self.headers,
                    timeout=10
                )
                response.raise_for_status()
                
                data = response.json()
                vacancies = data.get('items', [])
                
                print(f"Найдено вакансий на странице: {len(vacancies)}")
                
                parsed_vacancies = [self._parse_vacancy_item(item) for item in vacancies]
                all_vacancies.extend(parsed_vacancies)
                
                if data.get('pages', 0) <= page + 1:
                    print(" Больше страниц нет")
                    break
                
                # Задержка, чтобы не нагружать API
                time.sleep(0.5)
                
            except requests.exceptions.RequestException as e:
                print(f" Ошибка запроса: {e}")
                break
            except Exception as e:
                print(f" Неожиданная ошибка: {e}")
                break
        
        print(f" Всего собрано вакансий: {len(all_vacancies)}")
        return all_vacancies
    
    def _parse_vacancy_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """
        Преобразует элемент из API HH в наш формат
        
        Args:
            item: словарь с данными вакансии от HH.ru
        
        Returns:
            Dict в формате проекта
        """
        return {
            'title': item.get('name', 'Без названия'),
            'company': item.get('employer', {}).get('name', 'Не указана'),
            'salary_raw': str(item.get('salary')),
            'description': item.get('snippet', {}).get('requirement', ''),
            'skills': [],  # HH не дает навыков отдельно
            'url': item.get('alternate_url', ''),
            'source': 'hh',
            'parsed_at': datetime.now().isoformat()
        }


if __name__ == "__main__":
    parser = HHParser()
    vacancies = parser.parse(query="Python", pages=2)
    
    if vacancies:
        filename = f"hh_python_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        parser.save_raw_data(vacancies, filename)
        
        print("\n Пример вакансии:")
        print(json.dumps(vacancies[0], ensure_ascii=False, indent=2))