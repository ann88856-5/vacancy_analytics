import requests
from bs4 import BeautifulSoup
import time
import json
from datetime import datetime
from typing import List, Dict, Any
from base_parser import BaseParser  # Убрали parsers. из импорта

class HabrCareerParser(BaseParser):
    
    def __init__(self):
        super().__init__("habr")
        self.base_url = "https://career.habr.com"
        self.search_url = f"{self.base_url}/vacancies"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
    
    def parse(self, query: str = "Python", pages: int = 2) -> List[Dict[str, Any]]:
        vacancies = []
        
        for page in range(1, pages + 1):
            print(f"🔍 Парсинг страницы {page}...")
            
            params = {
                'q': query,
                'page': page,
                'type': 'all'
            }
            
            try:
                response = requests.get(
                    self.search_url, 
                    params=params, 
                    headers=self.headers,
                    timeout=10
                )
                response.raise_for_status()
                
                soup = BeautifulSoup(response.text, 'html.parser')
                page_vacancies = self._parse_page(soup)
                vacancies.extend(page_vacancies)
                
                print(f"   Найдено {len(page_vacancies)} вакансий")
                time.sleep(2)
                
            except Exception as e:
                print(f"❌ Ошибка: {e}")
                continue
        
        print(f"✅ Всего собрано {len(vacancies)} вакансий")
        return vacancies
    
    def _parse_page(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        vacancies = []
        vacancy_cards = soup.find_all('div', class_='vacancy-card')
        
        for card in vacancy_cards:
            try:
                vacancy = self._parse_vacancy_card(card)
                if vacancy:
                    vacancies.append(vacancy)
            except Exception as e:
                print(f"⚠️ Ошибка: {e}")
                continue
        
        return vacancies
    
    def _parse_vacancy_card(self, card) -> Dict[str, Any]:
        title_elem = card.find('div', class_='vacancy-card__title')
        if not title_elem:
            return None
        
        link_elem = title_elem.find('a')
        title = link_elem.text.strip() if link_elem else "Без названия"
        relative_url = link_elem.get('href', '') if link_elem else ''
        full_url = self.base_url + relative_url if relative_url else ''
        
        company_elem = card.find('div', class_='vacancy-card__company')
        company = company_elem.text.strip() if company_elem else "Не указана"
        
        salary_elem = card.find('div', class_='vacancy-card__salary')
        salary_text = salary_elem.text.strip() if salary_elem else "Не указана"
        
        meta_elem = card.find('div', class_='vacancy-card__meta')
        meta_text = meta_elem.text.strip() if meta_elem else ""
        
        skills_elem = card.find('div', class_='vacancy-card__skills')
        skills = []
        if skills_elem:
            skill_tags = skills_elem.find_all('a', class_='vacancy-card__skill')
            skills = [skill.text.strip() for skill in skill_tags]
        
        return {
            'title': title,
            'company': company,
            'salary': salary_text,
            'meta': meta_text,
            'skills': skills,
            'url': full_url,
            'source': 'habr',
            'parsed_at': datetime.now().isoformat()
        }

if __name__ == "__main__":
    parser = HabrCareerParser()
    vacancies = parser.parse(query="Python", pages=1)
    
    if vacancies:
        parser.save_raw_data(vacancies, "test_habr.json")
        print("\n📋 Пример вакансии:")
        print(json.dumps(vacancies[0], ensure_ascii=False, indent=2))
