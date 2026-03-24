#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Скрипт для извлечения навыков из описаний вакансий и загрузки в БД
"""

import sys
import os
import re
from collections import Counter

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.connection import SessionLocal
from database.models import Vacancy, Skill, vacancy_skills


# Словарь навыков с категориями
SKILLS_DICT = {
    # Языки программирования
    'Python': 'language',
    'Java': 'language',
    'JavaScript': 'language',
    'C++': 'language',
    'C#': 'language',
    'Go': 'language',
    'Ruby': 'language',
    'PHP': 'language',
    'Swift': 'language',
    'Kotlin': 'language',
    'TypeScript': 'language',
    
    # Фреймворки
    'Django': 'framework',
    'Flask': 'framework',
    'FastAPI': 'framework',
    'Spring': 'framework',
    'React': 'framework',
    'Vue': 'framework',
    'Angular': 'framework',
    'TensorFlow': 'framework',
    'PyTorch': 'framework',
    
    # Базы данных
    'SQL': 'database',
    'PostgreSQL': 'database',
    'MySQL': 'database',
    'MongoDB': 'database',
    'Redis': 'database',
    'Oracle': 'database',
    
    # Инструменты
    'Git': 'tool',
    'Docker': 'tool',
    'Kubernetes': 'tool',
    'Linux': 'tool',
    'AWS': 'tool',
    'Azure': 'tool',
    'GCP': 'tool',
    'REST API': 'tool',
    'GraphQL': 'tool',
    
    # Аналитика
    'Pandas': 'analytics',
    'NumPy': 'analytics',
    'Scikit-learn': 'analytics',
    'Data Science': 'analytics',
    'ML': 'analytics',
    'AI': 'analytics',
}


def extract_skills_from_text(text):
    """Извлекает навыки из текста"""
    if not text:
        return []
    
    text_lower = text.lower()
    found_skills = []
    
    for skill, category in SKILLS_DICT.items():
        # Ищем точное вхождение (с учетом границ слова)
        pattern = r'\b' + re.escape(skill.lower()) + r'\b'
        if re.search(pattern, text_lower):
            found_skills.append(skill)
    
    return list(set(found_skills))  # убираем дубликаты


def extract_skills_from_all_vacancies():
    """Извлекает навыки из всех вакансий в БД"""
    db = SessionLocal()
    
    # Получаем все вакансии с описаниями
    vacancies = db.query(Vacancy).all()
    print(f"Найдено вакансий: {len(vacancies)}")
    
    # Собираем навыки по вакансиям
    vacancy_skills_map = {}
    all_skills = Counter()
    
    for vac in vacancies:
        # Собираем текст для анализа
        text = ""
        if vac.description:
            text += vac.description + " "
        if vac.title:
            text += vac.title
        
        skills = extract_skills_from_text(text)
        
        if skills:
            vacancy_skills_map[vac.id] = skills
            for skill in skills:
                all_skills[skill] += 1
    
    print(f"Вакансий с найденными навыками: {len(vacancy_skills_map)}")
    print(f"Найдено уникальных навыков: {len(all_skills)}")
    
    return vacancy_skills_map, all_skills


def save_skills_to_db(vacancy_skills_map, all_skills):
    """Сохраняет навыки в базу данных"""
    db = SessionLocal()
    
    try:
        # Сначала добавляем все навыки в таблицу skills
        skill_ids = {}
        for skill_name, count in all_skills.most_common():
            category = SKILLS_DICT.get(skill_name, 'other')
            
            # Проверяем, есть ли уже такой навык
            existing = db.query(Skill).filter(Skill.name == skill_name).first()
            if existing:
                skill_ids[skill_name] = existing.id
                print(f"Навык уже существует: {skill_name}")
            else:
                new_skill = Skill(name=skill_name, category=category)
                db.add(new_skill)
                db.flush()
                skill_ids[skill_name] = new_skill.id
                print(f"Добавлен навык: {skill_name} ({category})")
        
        # Теперь создаем связи вакансия-навык
        connection_count = 0
        for vacancy_id, skills in vacancy_skills_map.items():
            for skill_name in skills:
                skill_id = skill_ids.get(skill_name)
                if skill_id:
                    # Проверяем, нет ли уже такой связи
                    existing = db.execute(
                        vacancy_skills.select().where(
                            (vacancy_skills.c.vacancy_id == vacancy_id) &
                            (vacancy_skills.c.skill_id == skill_id)
                        )
                    ).first()
                    
                    if not existing:
                        db.execute(
                            vacancy_skills.insert().values(
                                vacancy_id=vacancy_id,
                                skill_id=skill_id
                            )
                        )
                        connection_count += 1
        
        db.commit()
        print(f"\nДобавлено навыков: {len(skill_ids)}")
        print(f"Создано связей: {connection_count}")
        
    except Exception as e:
        db.rollback()
        print(f"Ошибка: {e}")
        raise
    finally:
        db.close()


def main():
    """Основная функция"""
    print("=" * 60)
    print("ИЗВЛЕЧЕНИЕ НАВЫКОВ ИЗ ОПИСАНИЙ ВАКАНСИЙ")
    print("=" * 60)
    
    vacancy_skills_map, all_skills = extract_skills_from_all_vacancies()
    
    print("\nТОП-10 НАЙДЕННЫХ НАВЫКОВ:")
    for i, (skill, count) in enumerate(all_skills.most_common(10), 1):
        print(f"  {i}. {skill}: {count} вакансий")
    
    print("\nСохраняем навыки в базу данных...")
    save_skills_to_db(vacancy_skills_map, all_skills)
    
    print("\n ГОТОВО!")


if __name__ == "__main__":
    main()