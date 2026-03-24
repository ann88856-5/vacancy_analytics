import sys
import os
import json
import pandas as pd
from sqlalchemy import text

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.connection import engine


class CompanyAnalyzer:
    """Класс для анализа компаний-работодателей"""

    def __init__(self):
        self.engine = engine
        self.companies_df = None
        self.industry_df = None

    def load_data(self):
        """
        Получение списка всех компаний из таблицы companies
        Подсчет количества вакансий по каждой компании
        Вычисление средней, минимальной и максимальной зарплаты по компаниям
        """
        print("Загрузка данных о компаниях...")

        query = text("""
            SELECT 
                c.id,
                c.name as company_name,
                COUNT(v.id) as vacancies_count,
                AVG((v.salary_min + v.salary_max)/2) as avg_salary,
                MIN(v.salary_min) as min_salary,
                MAX(v.salary_max) as max_salary,
                SUM(CASE WHEN v.salary_min IS NOT NULL THEN 1 ELSE 0 END) as vacancies_with_salary
            FROM companies c
            LEFT JOIN vacancies v ON c.id = v.company_id
            GROUP BY c.id, c.name
            ORDER BY vacancies_count DESC
        """)

        with self.engine.connect() as conn:
            self.companies_df = pd.read_sql(query, conn)

        print(f"Загружено компаний: {len(self.companies_df)}")
        return self.companies_df

    def get_top_companies_by_vacancies(self, n=10):
        """Определение компаний с наибольшим количеством вакансий"""
        if self.companies_df is None:
            self.load_data()
        return self.companies_df.head(n).copy()

    def get_top_companies_by_salary(self, n=10, min_vacancies=1):
        """Определение компаний с самыми высокими зарплатами"""
        if self.companies_df is None:
            self.load_data()

        filtered = self.companies_df[
            (self.companies_df['vacancies_count'] >= min_vacancies) &
            (self.companies_df['avg_salary'].notna())
        ]
        filtered = filtered.sort_values('avg_salary', ascending=False)
        return filtered.head(n).copy()

    def group_companies_by_industry(self):
        """
        Группировка компаний по сферам деятельности
        IT, банки, ритейл, другие
        """
        if self.companies_df is None:
            self.load_data()

        # Словарь ключевых слов для определения сферы
        industry_keywords = {
            'IT / Технологии': [
                'tech', 'digital', 'it', 'software', 'code', 'data', 'ai', 'ml',
                'cloud', 'system', 'dev', 'lab', 'solution', 'codemate', 'yoloprice',
                'p|e', 'itk', 'app', 'redlab', 'digital', 'innovative'
            ],
            'Финансы / Банки': [
                'bank', 'сбер', 'sber', 'finance', 'invest', 'т-банк', 't-bank',
                'vtb', 'втб', 'альфа', 'alpha', 'fin'
            ],
            'Ритейл / Торговля': [
                'retail', 'shop', 'market', 'магазин', 'торг', 'fix price',
                'ozon', 'wildberries', 'wb', 'x5', 'пятёрочка'
            ],
            'Консалтинг / Аутсорсинг': [
                'consult', 'outsource', 'solution', 'service', 'сервис'
            ],
            'Медиа / Образование': [
                'media', 'edu', 'school', 'academy', 'курс', 'habr', 'career'
            ],
            'Госсектор / Оборона': [
                'гос', 'state', 'defense', 'security', 'безопасность'
            ]
        }

        def determine_industry(company_name):
            """Определяет сферу компании по названию"""
            if not company_name:
                return 'Другое'

            name_lower = company_name.lower()

            for industry, keywords in industry_keywords.items():
                for keyword in keywords:
                    if keyword.lower() in name_lower:
                        return industry

            return 'Другое'

        # Добавляем колонку сферы
        self.companies_df['industry'] = self.companies_df['company_name'].apply(determine_industry)

        # Группируем по сферам
        industry_stats = self.companies_df.groupby('industry').agg({
            'company_name': 'count',
            'vacancies_count': 'sum',
            'avg_salary': 'mean'
        }).rename(columns={
            'company_name': 'companies_count',
            'vacancies_count': 'total_vacancies',
            'avg_salary': 'avg_salary_by_industry'
        }).round(0)

        industry_stats = industry_stats.sort_values('total_vacancies', ascending=False)

        self.industry_df = industry_stats
        return industry_stats

    def calculate_market_stats(self):
        """Общая статистика по рынку"""
        if self.companies_df is None:
            self.load_data()

        total_companies = len(self.companies_df)
        active_companies = len(self.companies_df[self.companies_df['vacancies_count'] > 0])
        total_vacancies = self.companies_df['vacancies_count'].sum()
        avg_vacancies_per_company = self.companies_df['vacancies_count'].mean()

        salary_data = self.companies_df.dropna(subset=['avg_salary'])

        stats = {
            'total_companies': int(total_companies),
            'active_companies': int(active_companies),
            'total_vacancies': int(total_vacancies),
            'avg_vacancies_per_company': round(float(avg_vacancies_per_company), 2),
            'companies_with_salary_data': int(len(salary_data)),
            'avg_salary_all': round(float(salary_data['avg_salary'].mean()), 2) if len(salary_data) > 0 else 0,
            'max_salary_all': round(float(salary_data['max_salary'].max()), 2) if len(salary_data) > 0 else 0,
            'min_salary_all': round(float(salary_data['min_salary'].min()), 2) if len(salary_data) > 0 else 0
        }

        return stats

    def save_results(self):
        """
        Сохранение результатов в JSON и CSV
        """
        if self.companies_df is None:
            self.load_data()

        os.makedirs('analytics/reports', exist_ok=True)

        # Топ компаний по вакансиям
        top_vacancies = self.get_top_companies_by_vacancies(15)
        top_vacancies.to_csv('analytics/reports/top_companies_by_vacancies.csv', index=False)

        # Топ компаний по зарплатам
        top_salary = self.get_top_companies_by_salary(10, min_vacancies=2)
        if len(top_salary) > 0:
            top_salary.to_csv('analytics/reports/top_companies_by_salary.csv', index=False)

        # Группировка по сферам
        industry_stats = self.group_companies_by_industry()
        industry_stats.to_csv('analytics/reports/companies_by_industry.csv')

        # Сохраняем в JSON
        companies_json = self.companies_df.head(20).to_dict('records')
        with open('analytics/reports/companies_stats.json', 'w', encoding='utf-8') as f:
            json.dump(companies_json, f, ensure_ascii=False, indent=2)

        # Статистика по сферам в JSON
        industry_json = industry_stats.reset_index().to_dict('records')
        with open('analytics/reports/industry_stats.json', 'w', encoding='utf-8') as f:
            json.dump(industry_json, f, ensure_ascii=False, indent=2)

        # Общая статистика
        market_stats = self.calculate_market_stats()
        with open('analytics/reports/market_stats.json', 'w', encoding='utf-8') as f:
            json.dump(market_stats, f, ensure_ascii=False, indent=2)

        print("Результаты сохранены в папку analytics/reports/")

    def print_report(self):
        """Выводит отчет в консоль"""
        if self.companies_df is None:
            self.load_data()

        stats = self.calculate_market_stats()
        industry_stats = self.group_companies_by_industry()

        print("\n" + "=" * 60)
        print("ОТЧЕТ ПО КОМПАНИЯМ-РАБОТОДАТЕЛЯМ")
        print("=" * 60)

        print("\nОБЩАЯ СТАТИСТИКА:")
        print(f"  Всего компаний в базе: {stats['total_companies']}")
        print(f"  Активных компаний (есть вакансии): {stats['active_companies']}")
        print(f"  Всего вакансий: {stats['total_vacancies']}")
        print(f"  Среднее количество вакансий на компанию: {stats['avg_vacancies_per_company']}")

        print("\nСТАТИСТИКА ПО ЗАРПЛАТАМ:")
        print(f"  Средняя зарплата по рынку: {stats['avg_salary_all']:,.0f} руб.")
        print(f"  Максимальная зарплата: {stats['max_salary_all']:,.0f} руб.")
        print(f"  Минимальная зарплата: {stats['min_salary_all']:,.0f} руб.")

        print("\nТОП-10 КОМПАНИЙ ПО КОЛИЧЕСТВУ ВАКАНСИЙ:")
        top10 = self.get_top_companies_by_vacancies(10)
        for i, (_, row) in enumerate(top10.iterrows(), 1):
            vacancies = int(row['vacancies_count'])
            avg_salary = row['avg_salary']
            if pd.notna(avg_salary) and avg_salary > 0:
                salary_str = f"{int(avg_salary):,.0f} руб."
            else:
                salary_str = "нет данных"
            print(f"  {i}. {row['company_name']}: {vacancies} вакансий, средняя зп {salary_str}")

        print("\nТОП-5 КОМПАНИЙ ПО УРОВНЮ ЗАРПЛАТ:")
        top_salary = self.get_top_companies_by_salary(5, min_vacancies=2)
        if len(top_salary) > 0:
            for i, (_, row) in enumerate(top_salary.iterrows(), 1):
                avg_salary = int(row['avg_salary'])
                vacancies = int(row['vacancies_count'])
                print(f"  {i}. {row['company_name']}: {avg_salary:,.0f} руб. ({vacancies} вакансий)")
        else:
            print("  Недостаточно данных для анализа зарплат")

        print("\nРАСПРЕДЕЛЕНИЕ КОМПАНИЙ ПО СФЕРАМ ДЕЯТЕЛЬНОСТИ:")
        for industry, row in industry_stats.iterrows():
            companies = int(row['companies_count'])
            vacancies = int(row['total_vacancies'])
            avg_salary = row['avg_salary_by_industry']
            if pd.notna(avg_salary) and avg_salary > 0:
                salary_str = f"{int(avg_salary):,.0f} руб."
            else:
                salary_str = "нет данных"
            print(f"  {industry}: {companies} компаний, {vacancies} вакансий, средняя зп {salary_str}")

        print("\n" + "=" * 60)


def main():
    """Основная функция"""
    print("=" * 60)
    print("АНАЛИЗ КОМПАНИЙ-РАБОТОДАТЕЛЕЙ")
    print("=" * 60)

    analyzer = CompanyAnalyzer()
    analyzer.load_data()
    analyzer.print_report()
    analyzer.save_results()


if __name__ == "__main__":
    main()
