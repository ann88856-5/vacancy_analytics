#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import matplotlib
matplotlib.use('Agg')
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import text

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.connection import engine


def plot_top_skills():
    """
    Построение столбчатой диаграммы топ-10 навыков
    """
    print("Построение диаграммы топ-10 навыков...")
    
    query = """
        SELECT s.name, COUNT(vs.vacancy_id) as count
        FROM skills s
        JOIN vacancy_skills vs ON s.id = vs.skill_id
        GROUP BY s.name
        ORDER BY count DESC
        LIMIT 10
    """
    
    df = pd.read_sql(query, engine)
    
    plt.figure(figsize=(12, 8))
    bars = plt.bar(df['name'], df['count'])
    plt.title('Топ-10 наиболее востребованных навыков', fontsize=16)
    plt.xlabel('Навык', fontsize=12)
    plt.ylabel('Количество вакансий', fontsize=12)
    plt.xticks(rotation=45, ha='right')
    
    # Подписи значений на столбцах
    for bar, count in zip(bars, df['count']):
        plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                 str(int(count)), ha='center', va='bottom')
    
    plt.tight_layout()
    plt.savefig('analytics/reports/top_skills_chart.png', dpi=150)
    plt.close()
    print("Сохранено: analytics/reports/top_skills_chart.png")


def plot_salary_distribution():
    """
    Построение гистограммы распределения зарплат
    """
    print("Построение гистограммы распределения зарплат...")
    
    query = """
        SELECT salary_min, salary_max
        FROM vacancies
        WHERE salary_min IS NOT NULL
    """
    
    df = pd.read_sql(query, engine)
    
    if df.empty:
        print("Нет данных о зарплатах")
        return
    
    # Вычисляем среднюю зарплату
    df['avg_salary'] = (df['salary_min'] + df['salary_max']) / 2
    
    plt.figure(figsize=(12, 8))
    plt.hist(df['avg_salary'], bins=20, edgecolor='black', alpha=0.7)
    plt.title('Распределение зарплат', fontsize=16)
    plt.xlabel('Зарплата (руб.)', fontsize=12)
    plt.ylabel('Количество вакансий', fontsize=12)
    plt.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig('analytics/reports/salary_distribution.png', dpi=150)
    plt.close()
    print("Сохранено: analytics/reports/salary_distribution.png")


def plot_top_companies():
    """
    Построение столбчатой диаграммы топ-10 компаний по количеству вакансий
    """
    print("Построение диаграммы топ-10 компаний...")
    
    query = """
        SELECT c.name, COUNT(v.id) as vacancies_count
        FROM companies c
        JOIN vacancies v ON c.id = v.company_id
        GROUP BY c.name
        ORDER BY vacancies_count DESC
        LIMIT 10
    """
    
    df = pd.read_sql(query, engine)
    
    plt.figure(figsize=(12, 8))
    bars = plt.barh(df['name'], df['vacancies_count'])
    plt.title('Топ-10 компаний по количеству вакансий', fontsize=16)
    plt.xlabel('Количество вакансий', fontsize=12)
    plt.ylabel('Компания', fontsize=12)
    
    # Подписи значений
    for bar, count in zip(bars, df['vacancies_count']):
        plt.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height()/2,
                 str(int(count)), ha='left', va='center')
    
    plt.tight_layout()
    plt.savefig('analytics/reports/top_companies.png', dpi=150)
    plt.close()
    print("Сохранено: analytics/reports/top_companies.png")


def main():
    """
    Основная функция - строит все графики
    """
    print("=" * 60)
    print("ГЕНЕРАЦИЯ ГРАФИКОВ")
    print("=" * 60)
    
    # Создаем папку для отчетов, если её нет
    os.makedirs('analytics/reports', exist_ok=True)
    
    # Строим все графики
    plot_top_skills()
    plot_salary_distribution()
    plot_top_companies()
    
    print("\n" + "=" * 60)
    print("Все графики сохранены в папку analytics/reports/")
    print("=" * 60)


if __name__ == "__main__":
    main()
