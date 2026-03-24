"""
Модуль для анализа навыков из вакансий
"""
import sys
import os

# Добавляем корневую папку проекта в путь поиска модулей
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import json
import os
from collections import Counter
from itertools import combinations
from sqlalchemy import text

from database.connection import engine


class SkillAnalyzer:
    """Класс для анализа навыков из базы данных"""

    def __init__(self):
        self.engine = engine
        self.skills_df = None
        self.skills_by_vacancy = None

    def load_data(self):
        """Загружает данные о навыках из БД"""
        print("Загрузка данных о навыках...")

        query_skills = text("""
            SELECT s.id, s.name, s.category, COUNT(vs.vacancy_id) as count
            FROM skills s
            LEFT JOIN vacancy_skills vs ON s.id = vs.skill_id
            GROUP BY s.id, s.name, s.category
            ORDER BY count DESC
        """)

        with self.engine.connect() as conn:
            self.skills_df = pd.read_sql(query_skills, conn)

        query_pairs = text("""
            SELECT v.id as vacancy_id, s.name as skill
            FROM vacancies v
            JOIN vacancy_skills vs ON v.id = vs.vacancy_id
            JOIN skills s ON vs.skill_id = s.id
            ORDER BY v.id
        """)

        with self.engine.connect() as conn:
            self.skills_by_vacancy = pd.read_sql(query_pairs, conn)

        print(f"Загружено навыков: {len(self.skills_df)}")
        print(f"Загружено связей: {len(self.skills_by_vacancy)}")

        return self.skills_df

    def get_top_skills(self, n=20):
        """Возвращает топ N навыков"""
        if self.skills_df is None:
            self.load_data()
        return self.skills_df.head(n).copy()

    def get_skill_categories(self):
        """Анализирует распределение навыков по категориям"""
        if self.skills_df is None:
            self.load_data()

        categories = self.skills_df.groupby('category').agg({
            'name': 'count',
            'count': 'sum'
        }).rename(columns={'name': 'unique_skills', 'count': 'total_mentions'})

        return categories.sort_values('total_mentions', ascending=False)

    def get_skill_combinations(self, top_n=10):
        """Анализирует, какие навыки чаще всего встречаются вместе"""
        if self.skills_by_vacancy is None:
            self.load_data()

        vacancy_skills = {}
        for _, row in self.skills_by_vacancy.iterrows():
            vac_id = row['vacancy_id']
            skill = row['skill']
            if vac_id not in vacancy_skills:
                vacancy_skills[vac_id] = []
            vacancy_skills[vac_id].append(skill)

        pairs_counter = Counter()
        for skills in vacancy_skills.values():
            if len(skills) >= 2:
                for pair in combinations(sorted(skills), 2):
                    pairs_counter[pair] += 1

        return pairs_counter.most_common(top_n)

    def save_results(self):
        """Сохраняет результаты анализа в файлы"""
        if self.skills_df is None:
            self.load_data()

        os.makedirs('analytics/reports', exist_ok=True)

        top_skills = self.get_top_skills(20)
        top_skills.to_csv('analytics/reports/top_skills.csv', index=False)
        with open('analytics/reports/top_skills.json', 'w', encoding='utf-8') as f:
            json.dump(top_skills.to_dict('records'), f, ensure_ascii=False, indent=2)

        categories = self.get_skill_categories()
        categories.to_csv('analytics/reports/skill_categories.csv')

        combinations = self.get_skill_combinations(10)
        comb_list = [{'skill1': p[0][0], 'skill2': p[0][1], 'count': p[1]}
                     for p in combinations]
        with open('analytics/reports/skill_combinations.json', 'w', encoding='utf-8') as f:
            json.dump(comb_list, f, ensure_ascii=False, indent=2)

        print("Результаты сохранены в analytics/reports/")

    def print_report(self):
        """Выводит отчет в консоль"""
        if self.skills_df is None:
            self.load_data()

        print("\n" + "=" * 60)
        print("ОТЧЕТ ПО АНАЛИЗУ НАВЫКОВ")
        print("=" * 60)

        print(f"\nВсего уникальных навыков: {len(self.skills_df)}")
        print(f"Всего упоминаний навыков: {self.skills_df['count'].sum()}")

        print("\nТОП-10 НАВЫКОВ:")
        top10 = self.get_top_skills(10)
        for i, (_, row) in enumerate(top10.iterrows(), 1):
            print(f"  {i}. {row['name']}: {int(row['count'])} вакансий")

        print("\nРАСПРЕДЕЛЕНИЕ ПО КАТЕГОРИЯМ:")
        categories = self.get_skill_categories()
        for category, row in categories.iterrows():
            print(f"  {category}: {int(row['total_mentions'])} упоминаний, {int(row['unique_skills'])} навыков")

        print("\nТОП-5 СОЧЕТАНИЙ НАВЫКОВ:")
        combinations = self.get_skill_combinations(5)
        for i, ((s1, s2), count) in enumerate(combinations, 1):
            print(f"  {i}. {s1} + {s2}: {count} вакансий")


if __name__ == "__main__":
    analyzer = SkillAnalyzer()
    analyzer.load_data()
    analyzer.print_report()
    analyzer.save_results()
