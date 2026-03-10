"""
Модуль для очистки данных о вакансиях
"""

import json
import re
import os
from typing import Dict, List, Any


def clean_salary(salary_text: str) -> Dict[str, Any]:
    """
    Преобразует текстовое описание зарплаты в числа.
    """
    # Пока заглушка
    print(f"Очищаю зарплату: {salary_text}")
    return {"min": None, "max": None, "currency": None}


def remove_html_tags(text: str) -> str:
    """
    Удаляет HTML-теги из текста.
    """
    if not text:
        return ""
    
    # Удаляем все теги вида <...>
    clean_text = re.sub(r'<[^>]+>', '', text)
    
    # Заменяем множественные пробелы на один
    clean_text = re.sub(r'\s+', ' ', clean_text)
    
    return clean_text.strip()


def clean_vacancy(vacancy: Dict[str, Any]) -> Dict[str, Any]:
    """
    Очищает одну вакансию.
    """
    print(f"Очищаю вакансию: {vacancy.get('title', 'Без названия')}")
    return vacancy


def clean_json_file(input_path: str, output_path: str) -> int:
    """
    Читает JSON с вакансиями, очищает каждую и сохраняет результат.
    """
    print(f"Читаю файл: {input_path}")
    
    with open(input_path, 'r', encoding='utf-8') as f:
        vacancies = json.load(f)
    
    print(f"Найдено вакансий: {len(vacancies)}")
    
    cleaned_vacancies = []
    for i, vacancy in enumerate(vacancies):
        cleaned = clean_vacancy(vacancy)
        cleaned_vacancies.append(cleaned)
    
    # Папка для выходного файла
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(cleaned_vacancies, f, ensure_ascii=False, indent=2)
    
    print(f"Сохранено в: {output_path}")
    return len(cleaned_vacancies)


if __name__ == "__main__":
    input_file = "data/raw/test_habr.json"
    output_file = "data/processed/habr_clean.json"
    
    if os.path.exists(input_file):
        count = clean_json_file(input_file, output_file)
        print(f"Обработано вакансий: {count}")
    else:
        print(f"Файл {input_file} не найден")
        import glob
        json_files = glob.glob("data/raw/*.json")
        if json_files:
            input_file = json_files[0]
            print(f"Найден альтернативный файл: {input_file}")
            count = clean_json_file(input_file, output_file)
            print(f"Обработано вакансий: {count}")
