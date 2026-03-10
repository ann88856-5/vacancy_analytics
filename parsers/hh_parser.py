"""
Парсер для HeadHunter API
Документация: https://github.com/hhru/api
"""

import requests
import time
import json
import os
import sys
import re
from datetime import datetime
from typing import List, Dict, Any

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from parsers.base_parser import BaseParser


class HHParser(BaseParser):
    """
    Парсер для сбора вакансий с HeadHunter через API
    """
    
    def __init__(self):
        super().__init__("hh")
        self.base_url = "https://api.hh.ru/vacancies"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }

    def extract_skills_from_description(self, description: str) -> List[str]:
        """
        Извлекает навыки из текста описания
        """
        if not description:
            return []
        
        common_skills = [
            'Python', 'Django', 'Flask', 'FastAPI', 'SQL', 'PostgreSQL',
            'MySQL', 'Git', 'Docker', 'Linux', 'JavaScript', 'React',
            'HTML', 'CSS', 'Redis', 'MongoDB', 'Celery', 'Pandas',
            'NumPy', 'Scikit-learn', 'TensorFlow', 'PyTorch', 'Java',
            'C++', 'C#', 'PHP', 'Ruby', 'Go', 'Rust', 'Kubernetes',
            'AWS', 'Azure', 'GCP', 'REST', 'API', 'GraphQL'
        ]
        
        found_skills = []
        desc_lower = description.lower()
        
        for skill in common_skills:
            if skill.lower() in desc_lower:
                found_skills.append(skill)
        
        return found_skills        
    
    def parse(self, query: str = "Python", pages: int = 5, area: int = 1) -> List[Dict[str, Any]]:
        """
        Парсит вакансии с HeadHunter API
        
        Args:
            query: поисковый запрос
            pages: максимальное количество страниц
            area: регион (1 - Москва, 2 - СПб, 113 - Россия)
        
        Returns:
            List[Dict] - список вакансий
        """
        print(f"Поиск вакансий по запросу: '{query}' в регионе {area}")
        
        all_vacancies = []
        total_found = 0
        
        for page in range(pages):
            print(f"Загрузка страницы {page + 1}/{pages}...")
            
            params = {
                'text': query,
                'area': area,
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
                
                # Проверка на ошибки HTTP
                if response.status_code == 429:
                    print("Слишком много запросов, жду 10 секунд...")
                    time.sleep(10)
                    continue
                elif response.status_code != 200:
                    print(f"Ошибка HTTP: {response.status_code}")
                    print(response.text)
                    break
                
                data = response.json()
                
                # Общее количество найденных вакансий (из первого запроса)
                if page == 0:
                    total_found = data.get('found', 0)
                    print(f"Всего найдено вакансий: {total_found}")
                
                vacancies = data.get('items', [])
                print(f"Получено вакансий: {len(vacancies)}")
                
                for item in vacancies:
                    try:
                        parsed = self._parse_vacancy_item(item)
                        all_vacancies.append(parsed)
                    except Exception as e:
                        print(f"Ошибка парсинга вакансии: {e}")
                        continue
                
                if data.get('pages', 0) <= page + 1:
                    print("Достигнут конец списка")
                    break
                
                # Задержка, чтобы не нагружать API
                time.sleep(0.5)
                
            except requests.exceptions.Timeout:
                print("Таймаут запроса")
                continue
            except requests.exceptions.ConnectionError:
                print("Ошибка соединения")
                break
            except Exception as e:
                print(f"Неожиданная ошибка: {e}")
                break
        
        print(f"Успешно собрано вакансий: {len(all_vacancies)} из {total_found}")
        return all_vacancies
    
    def _parse_vacancy_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """
        Преобразует элемент из API HH в наш формат
        """
        title = item.get('name', 'Без названия')
        company = item.get('employer', {}).get('name', 'Не указана')
        
        salary = item.get('salary')
        salary_min = None
        salary_max = None
        currency = None
        salary_raw = "Не указана"
        
        if salary:
            salary_min = salary.get('from')
            salary_max = salary.get('to')
            currency = salary.get('currency', 'RUR')
            
            if currency == 'RUR':
                currency = 'RUB'
            
            if salary_min and salary_max:
                salary_raw = f"{salary_min} - {salary_max} {currency}"
            elif salary_min:
                salary_raw = f"от {salary_min} {currency}"
            elif salary_max:
                salary_raw = f"до {salary_max} {currency}"
        
        # Описание
        description = item.get('snippet', {}).get('requirement', '')
        if description:
            description = re.sub(r'<[^>]+>', '', description)
        
        # URL
        url = item.get('alternate_url', '')
        published_at = item.get('published_at')
        
        return {
            'title': title,
            'company': company,
            'salary_min': salary_min,
            'salary_max': salary_max,
            'currency': currency,
            'salary_raw': salary_raw,
            'description': description,
            'skills': self.extract_skills_from_description(description),
            'url': url,
            'source': 'hh',
            'published_at': published_at,
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