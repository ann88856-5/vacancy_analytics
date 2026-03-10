"""
Тестирование парсера HeadHunter
"""

import json
from parsers.hh_parser import HHParser

def test_hh_parser():
    """Тестирует работу парсера HH"""
    
    print("=" * 50)
    print("ТЕСТИРОВАНИЕ ПАРСЕРА HEADHUNTER")
    print("=" * 50)
    
    parser = HHParser()
    
    queries = ["Python", "Java", "数据分析"]  # последний для проверки Unicode
    
    for query in queries:
        print(f"\nПоиск: '{query}'")
        vacancies = parser.parse(query=query, pages=1, area=1)
        
        if vacancies:
            print(f"Найдено вакансий: {len(vacancies)}")
            
            salaries = [v for v in vacancies if v['salary_min'] or v['salary_max']]
            print(f"С зарплатой: {len(salaries)}/{len(vacancies)}")
            
            all_skills = []
            for v in vacancies:
                all_skills.extend(v.get('skills', []))
            
            if all_skills:
                from collections import Counter
                top_skills = Counter(all_skills).most_common(5)
                print(f"Топ навыков: {top_skills}")
            
            filename = f"test_hh_{query}.json"
            with open(f"data/raw/{filename}", 'w', encoding='utf-8') as f:
                json.dump(vacancies, f, ensure_ascii=False, indent=2)
            print(f"Сохранено в data/raw/{filename}")
        else:
            print("Вакансий не найдено")
        
        print("-" * 30)

if __name__ == "__main__":
    test_hh_parser()