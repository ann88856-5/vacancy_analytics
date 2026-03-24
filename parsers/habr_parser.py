import requests
from bs4 import BeautifulSoup
import time
import json
from datetime import datetime
from typing import List, Dict, Any, Optional
from base_parser import BaseParser


class HabrCareerParser(BaseParser):

    def __init__(self):
        super().__init__("habr")
        self.base_url = "https://career.habr.com"
        self.search_url = f"{self.base_url}/vacancies"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }

    def parse_vacancy_details(self, url: str) -> Dict[str, Any]:
        """
        Parses detailed vacancy page to get description
        """
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Ищем описание вакансии - пробуем разные варианты
            description = ""
            
            # Вариант 1: ищем div с классом vacancy-description
            desc_elem = soup.find('div', class_='vacancy-description')
            if desc_elem:
                description = desc_elem.text.strip()
            
            # Вариант 2: если не нашли, ищем по другому классу
            if not description:
                desc_elem = soup.find('div', class_='job-description')
                if desc_elem:
                    description = desc_elem.text.strip()
            
            # Вариант 3: ищем блок с описанием по содержимому
            if not description:
                # Ищем все div, которые могут содержать описание
                content_divs = soup.find_all('div', class_=['content', 'description', 'vacancy-description__text'])
                for div in content_divs:
                    if len(div.text.strip()) > 100:  # Если текст достаточно длинный
                        description = div.text.strip()
                        break
            
            # Вариант 4: ищем по тегам (описание часто в h3 + следующий div)
            if not description:
                headers = soup.find_all(['h2', 'h3'])
                for header in headers:
                    if 'описание' in header.text.lower():
                        next_elem = header.find_next_sibling()
                        if next_elem:
                            description = next_elem.text.strip()
                            break
            
            print(f"   Found description length: {len(description)}")
            
            return {'description': description}
            
        except Exception as e:
            print(f"Error parsing details for {url}: {e}")
            return {'description': ''}
    
    def parse(self, query: str = "Python", pages: int = 2) -> List[Dict[str, Any]]:
        all_vacancies = []
        for page in range(1, pages + 1):
            print(f"Parsing page {page}...")

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
                
                # Parse details for each vacancy
                print(f"   Fetching details for {len(page_vacancies)} vacancies...")
                for idx, vacancy in enumerate(page_vacancies):
                    if vacancy.get('url'):
                        details = self.parse_vacancy_details(vacancy['url'])
                        vacancy['description'] = details.get('description', '')
                    if (idx + 1) % 10 == 0:
                        print(f"      Processed {idx + 1}/{len(page_vacancies)}")
                    time.sleep(0.5)  # Small delay between detail requests
                
                all_vacancies.extend(page_vacancies)
                print(f"   Found {len(page_vacancies)} vacancies with descriptions")
                time.sleep(2)

            except Exception as e:
                print(f"Error: {e}")
                continue

        print(f"Total collected: {len(all_vacancies)} vacancies")
        return all_vacancies

    def _parse_page(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        page_vacancies = []  # Переименовано для ясности
        vacancy_cards = soup.find_all('div', class_='vacancy-card')

        for card in vacancy_cards:
            try:
                vacancy = self._parse_vacancy_card(card)
                if vacancy:
                    page_vacancies.append(vacancy)
            except Exception as e:
                print(f"⚠ Ошибка: {e}")
                continue

        return page_vacancies

    def _parse_vacancy_card(self, card) -> Optional[Dict[str, Any]]:  # Явно указан Optional
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

        vacancy_data: Dict[str, Any] = {
            'title': title,
            'company': company,
            'salary': salary_text,
            'meta': meta_text,
            'skills': skills,
            'url': full_url,
            'source': 'habr',
            'parsed_at': datetime.now().isoformat()
        }

        return vacancy_data


if __name__ == "__main__":
    parser = HabrCareerParser()
    vacancies = parser.parse(query="Python", pages=1)

    if vacancies:
        parser.save_raw_data(vacancies, "test_habr.json")
        print("\n Пример вакансии:")
        print(json.dumps(vacancies[0], ensure_ascii=False, indent=2))
