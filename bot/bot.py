import sys
import os
import requests
from telegram import Update
from telegram.ext import Application, CommandHandler

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from jokes import get_random_joke

TOKEN = "8257840014:AAG4_vGf9yPQ1jivz7gWffIvPYzmUZgc_5M"
API_URL = "http://localhost:8000"


async def start(update: Update, _):
    user = update.effective_user
    await update.message.reply_text(
        f"Привет, {user.first_name}!\n\n"
        "Я бот для анализа вакансий. Я умею:\n\n"
        "/vacancies - список вакансий\n"
        "/skills - топ-10 навыков\n"
        "/stats - общая статистика\n"
        "/companies - список компаний\n"
        "/joke - случайный IT-анекдот\n"
        "/help - помощь\n\n"
        "Используй фильтры: /vacancies?company=Сбер"
    )


async def help_command(update: Update, _):
    await update.message.reply_text(
        "Доступные команды:\n\n"
        "/vacancies - список вакансий\n"
        "  Пример: /vacancies?company=Сбер\n"
        "  Пример: /vacancies?skill=Python\n"
        "  Пример: /vacancies?salary_min=200000\n"
        "/skills - топ-10 навыков\n"
        "/stats - общая статистика\n"
        "/companies - список компаний\n"
        "/joke - случайный IT-анекдот\n"
        "/help - эта справка"
    )


async def get_vacancies(update: Update, _):
    text = update.message.text
    params = {}

    if '?company=' in text:
        company = text.split('?company=')[1].split()[0]
        params['company'] = company
    elif '?skill=' in text:
        skill = text.split('?skill=')[1].split()[0]
        params['skill'] = skill
    elif '?salary_min=' in text:
        salary_min = text.split('?salary_min=')[1].split()[0]
        params['salary_min'] = salary_min

    try:
        response = requests.get(f"{API_URL}/vacancies", params=params, timeout=10)
        response.raise_for_status()
        vacancies = response.json()

        if not vacancies:
            await update.message.reply_text("Вакансий не найдено")
            return

        message = "*Вакансии:*\n\n"
        for v in vacancies[:5]:
            company_name = v.get('company_name', 'Не указана')
            salary = ""
            if v.get('salary_min') and v.get('salary_max'):
                salary = f" {v['salary_min']} - {v['salary_max']} {v.get('currency', '')}\n"
            elif v.get('salary_min'):
                salary = f" от {v['salary_min']} {v.get('currency', '')}\n"
            elif v.get('salary_max'):
                salary = f" до {v['salary_max']} {v.get('currency', '')}\n"

            message += f"• *{v['title']}*\n"
            message += f"  Компания: {company_name}\n"
            message += salary
            message += f"  [Ссылка]({v['url']})\n\n"

        if len(vacancies) > 5:
            message += f"_Показано 5 из {len(vacancies)} вакансий_"

        await update.message.reply_text(message, parse_mode='Markdown')

    except requests.exceptions.RequestException as e:
        await update.message.reply_text(f"Ошибка при получении данных: {e}")


async def get_skills(update: Update, _):
    try:
        response = requests.get(f"{API_URL}/skills/top", timeout=10)
        response.raise_for_status()
        skills = response.json()

        if not skills:
            await update.message.reply_text("Навыки не найдены")
            return

        message = " *Топ-10 навыков:*\n\n"
        for i, skill in enumerate(skills[:10], 1):
            message += f"{i}. {skill['name']} — {skill['count']} вакансий\n"

        await update.message.reply_text(message, parse_mode='Markdown')

    except requests.exceptions.RequestException as e:
        await update.message.reply_text(f"Ошибка при получении данных: {e}")


async def get_stats(update: Update, _):
    try:
        response = requests.get(f"{API_URL}/stats", timeout=10)
        response.raise_for_status()
        stats = response.json()

        message = (
            " *Общая статистика:*\n\n"
            f"• Вакансий: {stats['total_vacancies']}\n"
            f"• Компаний: {stats['total_companies']}\n"
            f"• Навыков: {stats['total_skills']}\n"
            f"• Средняя зарплата: {stats['avg_salary']:,.0f} ₽\n"
            f"• Топ навык: {stats['top_skill']}\n\n"
            f" *Анекдот в подарок:*\n{get_random_joke()}"
        )

        await update.message.reply_text(message, parse_mode='Markdown')

    except requests.exceptions.RequestException as e:
        await update.message.reply_text(f"Ошибка при получении данных: {e}")


async def get_companies(update: Update, _):
    try:
        response = requests.get(f"{API_URL}/companies", timeout=10)
        response.raise_for_status()
        companies = response.json()

        if not companies:
            await update.message.reply_text("Компании не найдены")
            return

        message = "*Топ-10 компаний:*\n\n"
        for i, company in enumerate(companies[:10], 1):
            vacancies_count = company.get('vacancies_count', 0)
            avg_salary = company.get('avg_salary')
            salary_str = f" — {int(avg_salary):,.0f} ₽" if avg_salary else ""
            message += f"{i}. {company['name']} — {vacancies_count} вакансий{salary_str}\n"

        await update.message.reply_text(message, parse_mode='Markdown')

    except requests.exceptions.RequestException as e:
        await update.message.reply_text(f"Ошибка при получении данных: {e}")


async def joke(update: Update, _):
    await update.message.reply_text(f" {get_random_joke()}")


def main():
    application = Application.builder().token(TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("vacancies", get_vacancies))
    application.add_handler(CommandHandler("skills", get_skills))
    application.add_handler(CommandHandler("stats", get_stats))
    application.add_handler(CommandHandler("companies", get_companies))
    application.add_handler(CommandHandler("joke", joke))

    print("Бот запущен...")
    application.run_polling()


if __name__ == "__main__":
    main()
