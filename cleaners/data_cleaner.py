"""
Модуль для очистки данных о вакансиях
"""

import json
import re
import os
from typing import Dict, List, Any

def normalize_spaces(text: str) -> str:
    """
    Нормализует пробелы в тексте:
    - Добавляет пробелы между словами, написанными слитно
    """
    if not text:
        return ""
    
    import re
    text = re.sub(r'(?<=[a-zа-я])(?=[A-ZА-Я])', ' ', text)
    text = re.sub(r'(?<=[a-zа-я])(?=[0-9])', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    
    return text.strip()

def clean_salary(salary_text: str) -> Dict[str, Any]:
    """
    Преобразует текстовое описание зарплаты в числа.
    """
    if not salary_text or salary_text == "Не указана":
        return {"min": None, "max": None, "currency": None}
    
    text = salary_text.replace('\xa0', ' ').replace(' ', '')

    currency = "RUB"
    if '₽' in text or 'руб' in text.lower():
        currency = "RUB"
        text = re.sub(r'[₽руб]', '', text, flags=re.IGNORECASE)
    elif '$' in text:
        currency = "USD"
        text = text.replace('$', '')
    elif '€' in text:
        currency = "EUR"
        text = text.replace('€', '')
    
    numbers = re.findall(r'\d+', text)
    numbers = [int(n) for n in numbers]
    
    result = {"min": None, "max": None, "currency": currency}
    
    if not numbers:
        return result
    
    if len(numbers) == 1:
        if 'до' in salary_text.lower() and 'от' not in salary_text.lower():
            result["max"] = numbers[0]
        elif 'от' in salary_text.lower() and 'до' not in salary_text.lower():
            result["min"] = numbers[0]
        else:
            result["min"] = result["max"] = numbers[0]
    else:
        result["min"] = numbers[0]
        result["max"] = numbers[1]
    
    return result


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
    cleaned = vacancy.copy()
    
    if 'salary' in cleaned:
        salary_data = clean_salary(cleaned['salary'])
        cleaned['salary_min'] = salary_data['min']
        cleaned['salary_max'] = salary_data['max']
        cleaned['currency'] = salary_data['currency']
        cleaned['salary_raw'] = cleaned['salary']
        del cleaned['salary']
    
    if 'description' in cleaned and cleaned['description']:
        cleaned['description'] = remove_html_tags(cleaned['description'])
    
    if 'meta' in cleaned and cleaned['meta']:
        cleaned['meta'] = remove_html_tags(cleaned['meta'])
        cleaned['meta'] = normalize_spaces(cleaned['meta'])
    
    return cleaned

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
