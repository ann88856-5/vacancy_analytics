import sys
import os
import json
import glob

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.connection import SessionLocal
from database.models import Vacancy, Company, Skill, vacancy_skills


def load_hh_vacancies(json_path):
    """Загружает вакансии из JSON-файла HH в базу данных"""
    
    db = SessionLocal()
    
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            vacancies_data = json.load(f)
        
        print(f"Loading {len(vacancies_data)} vacancies from {json_path}")
        
        added_count = 0
        skipped_count = 0
        
        for vac_data in vacancies_data:
            # Проверяем, есть ли уже такая вакансия по URL
            existing = db.query(Vacancy).filter(Vacancy.url == vac_data.get('url')).first()
            if existing:
                skipped_count += 1
                continue
            
            # Создаем компанию, если её нет
            company_name = vac_data.get('company', 'Unknown')
            company = db.query(Company).filter(Company.name == company_name).first()
            if not company:
                company = Company(name=company_name)
                db.add(company)
                db.flush()
                print(f"  Added company: {company_name}")
            
            # Создаем вакансию
            vacancy = Vacancy(
                title=vac_data.get('title', ''),
                description=vac_data.get('description', ''),
                salary_min=vac_data.get('salary_min'),
                salary_max=vac_data.get('salary_max'),
                currency=vac_data.get('currency'),
                company_id=company.id,
                source=vac_data.get('source', 'hh'),
                url=vac_data.get('url')
            )
            db.add(vacancy)
            db.flush()
            
            # Добавляем навыки, если есть
            skills_list = vac_data.get('skills', [])
            for skill_name in skills_list:
                skill = db.query(Skill).filter(Skill.name == skill_name).first()
                if not skill:
                    skill = Skill(name=skill_name)
                    db.add(skill)
                    db.flush()
                
                # Создаем связь
                db.execute(
                    vacancy_skills.insert().values(
                        vacancy_id=vacancy.id,
                        skill_id=skill.id
                    )
                )
            
            added_count += 1
            if added_count % 10 == 0:
                print(f"  Loaded {added_count} vacancies...")
        
        db.commit()
        print(f"\nDone. Added: {added_count}, Skipped: {skipped_count}")
        
    except Exception as e:
        db.rollback()
        print(f"Error: {e}")
        raise
    finally:
        db.close()


def main():
    # Находим последний JSON-файл HH
    json_files = glob.glob("data/raw/hh_*.json")
    if not json_files:
        print("No HH JSON files found in data/raw/")
        return
    
    # Берем самый свежий
    latest_file = max(json_files, key=os.path.getctime)
    print(f"Using latest file: {latest_file}")
    
    load_hh_vacancies(latest_file)


if __name__ == "__main__":
    main()