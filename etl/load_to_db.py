"""
Модуль для загрузки очищенных данных в PostgreSQL
"""

import json
import os
import sys
from typing import Dict, List, Any

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.connection import SessionLocal
from database.models import Company, Vacancy, Skill, vacancy_skills


def load_companies(vacancies: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Загружает компании в БД и возвращает словарь {название: id}
    """
    print(" Загрузка компаний...")
    
    db = SessionLocal()
    companies_dict = {}
    
    try:
        unique_companies = set()
        for vacancy in vacancies:
            if 'company' in vacancy and vacancy['company']:
                unique_companies.add(vacancy['company'])
        
        print(f"   Найдено уникальных компаний: {len(unique_companies)}")
        
        for company_name in unique_companies:
            # Проверяем, есть ли уже такая компания
            existing = db.query(Company).filter(Company.name == company_name).first()
            
            if existing:
                companies_dict[company_name] = existing.id
                print(f"   ⏩ Компания уже существует: {company_name} (id: {existing.id})")
            else:
                new_company = Company(name=company_name)
                db.add(new_company)
                db.flush() 
                companies_dict[company_name] = new_company.id
                print(f"    Добавлена компания: {company_name} (id: {new_company.id})")
        
        db.commit()
        print(f" Загружено компаний: {len(companies_dict)}")
        
    except Exception as e:
        db.rollback()
        print(f" Ошибка при загрузке компаний: {e}")
        raise
    finally:
        db.close()
    
    return companies_dict


def load_skills(vacancies: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Загружает навыки в БД и возвращает словарь {название: id}
    """
    print(" Загрузка навыков...")
    
    db = SessionLocal()
    skills_dict = {}
    
    try:
        unique_skills = set()
        for vacancy in vacancies:
            if 'skills' in vacancy and vacancy['skills']:
                for skill_name in vacancy['skills']:
                    if skill_name:  
                        unique_skills.add(skill_name)
        
        print(f" Найдено уникальных навыков: {len(unique_skills)}")
        
        for skill_name in unique_skills:
            existing = db.query(Skill).filter(Skill.name == skill_name).first()
            
            if existing:
                skills_dict[skill_name] = existing.id
                print(f" Навык уже существует: {skill_name} (id: {existing.id})")
            else:
                new_skill = Skill(name=skill_name)
                db.add(new_skill)
                db.flush()  
                skills_dict[skill_name] = new_skill.id
                print(f" Добавлен навык: {skill_name} (id: {new_skill.id})")
        
        # Сохраняем изменения
        db.commit()
        print(f"Загружено навыков: {len(skills_dict)}")
        
    except Exception as e:
        db.rollback()
        print(f" Ошибка при загрузке навыков: {e}")
        raise
    finally:
        db.close()
    
    return skills_dict


def load_vacancies(vacancies: List[Dict[str, Any]], companies_dict: Dict[str, int]) -> Dict[str, int]:
    """
    Загружает вакансии в БД и возвращает словарь {url: id}
    """
    print(" Загрузка вакансий...")
    
    db = SessionLocal()
    vacancies_dict = {}
    
    try:
        loaded_count = 0
        skipped_count = 0
        
        for vacancy_data in vacancies:
            if 'url' in vacancy_data and vacancy_data['url']:
                existing = db.query(Vacancy).filter(Vacancy.url == vacancy_data['url']).first()
                if existing:
                    vacancies_dict[vacancy_data['url']] = existing.id
                    skipped_count += 1
                    continue
            
            company_id = None
            if 'company' in vacancy_data and vacancy_data['company']:
                company_id = companies_dict.get(vacancy_data['company'])
            
            new_vacancy = Vacancy(
                title=vacancy_data.get('title', 'Без названия'),
                description=vacancy_data.get('description', ''),
                salary_min=vacancy_data.get('salary_min'),
                salary_max=vacancy_data.get('salary_max'),
                currency=vacancy_data.get('currency', 'RUB'),
                company_id=company_id,
                source=vacancy_data.get('source', 'unknown'),
                url=vacancy_data.get('url'),
                published_at=None  
            )
            
            db.add(new_vacancy)
            db.flush()  
            
            if 'url' in vacancy_data and vacancy_data['url']:
                vacancies_dict[vacancy_data['url']] = new_vacancy.id
            
            loaded_count += 1
            
            if loaded_count % 10 == 0:
                print(f"   Загружено {loaded_count} вакансий...")
        
        db.commit()
        print(f" Загружено вакансий: {loaded_count}")
        print(f" Пропущено (уже были): {skipped_count}")
        
    except Exception as e:
        db.rollback()
        print(f" Ошибка при загрузке вакансий: {e}")
        raise
    finally:
        db.close()
    
    return vacancies_dict


def create_vacancy_skills(vacancies: List[Dict[str, Any]], 
                          vacancies_dict: Dict[str, int], 
                          skills_dict: Dict[str, int]) -> None:
    """
    Создает связи между вакансиями и навыками
    """
    print(" Создание связей вакансия-навык...")
    # Пока заглушка
    pass


def load_from_json(json_path: str) -> None:
    """
    Загружает данные из JSON файла в БД
    """
    print(f" Читаю файл: {json_path}")
    
    with open(json_path, 'r', encoding='utf-8') as f:
        vacancies = json.load(f)
    
    print(f" Найдено вакансий: {len(vacancies)}")

    db = SessionLocal()
    
    try:
        companies_dict = load_companies(vacancies)
        
        skills_dict = load_skills(vacancies)
        
        vacancies_dict = load_vacancies(vacancies, companies_dict)
        
        create_vacancy_skills(vacancies, vacancies_dict, skills_dict)
        
        db.commit()
        print(" Все данные успешно загружены в БД!")
        
    except Exception as e:
        db.rollback()
        print(f" Ошибка при загрузке: {e}")
        raise
    finally:
        db.close()


if __name__ == "__main__":
    json_file = "data/processed/habr_clean.json"
    
    if os.path.exists(json_file):
        load_from_json(json_file)
    else:
        print(f" Файл {json_file} не найден")
        import glob
        json_files = glob.glob("data/processed/*.json")
        if json_files:
            print(f"Найден альтернативный файл: {json_files[0]}")
            load_from_json(json_files[0])