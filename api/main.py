from fastapi import FastAPI, Depends, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from typing import Optional, List
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.connection import SessionLocal
from database.models import Vacancy, Company, Skill, vacancy_skills
from api.schemas import VacancyResponse, SkillResponse, CompanyResponse, StatsResponse

app = FastAPI(
    title="Vacancy Analytics API",
    version="1.0.0",
    description="API для доступа к данным о вакансиях, навыках и компаниях"
)

# Настройка CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.get("/")
def root():
    return {"message": "Vacancy Analytics API", "docs": "/docs"}


@app.get("/vacancies", response_model=List[VacancyResponse])
def get_vacancies(
    db: Session = Depends(get_db),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    company: Optional[str] = None,
    skill: Optional[str] = None,
    salary_min: Optional[int] = None,
    salary_max: Optional[int] = None
):
    query = db.query(Vacancy)
    
    if company:
        query = query.join(Company).filter(Company.name.ilike(f"%{company}%"))
    if skill:
        query = query.join(vacancy_skills).join(Skill).filter(Skill.name.ilike(f"%{skill}%"))
    if salary_min:
        query = query.filter(Vacancy.salary_max >= salary_min)
    if salary_max:
        query = query.filter(Vacancy.salary_min <= salary_max)
    
    total = query.count()
    vacancies = query.offset(offset).limit(limit).all()
    
    return vacancies


@app.get("/vacancies/{vacancy_id}", response_model=VacancyResponse)
def get_vacancy(vacancy_id: int, db: Session = Depends(get_db)):
    vacancy = db.query(Vacancy).filter(Vacancy.id == vacancy_id).first()
    if not vacancy:
        raise HTTPException(status_code=404, detail="Vacancy not found")
    return vacancy


@app.get("/skills", response_model=List[SkillResponse])
def get_skills(db: Session = Depends(get_db), limit: int = 100, offset: int = 0):
    skills = db.query(Skill).offset(offset).limit(limit).all()
    return skills


@app.get("/skills/top", response_model=List[SkillResponse])
def get_top_skills(db: Session = Depends(get_db), limit: int = 10):
    query = """
        SELECT s.id, s.name, s.category, COUNT(vs.vacancy_id) as count
        FROM skills s
        JOIN vacancy_skills vs ON s.id = vs.skill_id
        GROUP BY s.id, s.name, s.category
        ORDER BY count DESC
        LIMIT :limit
    """
    result = db.execute(query, {"limit": limit}).fetchall()
    return [{"id": r[0], "name": r[1], "category": r[2], "count": r[3]} for r in result]


@app.get("/companies", response_model=List[CompanyResponse])
def get_companies(db: Session = Depends(get_db), limit: int = 20, offset: int = 0):
    query = """
        SELECT c.id, c.name, COUNT(v.id) as vacancies_count, AVG((v.salary_min + v.salary_max)/2) as avg_salary
        FROM companies c
        LEFT JOIN vacancies v ON c.id = v.company_id
        GROUP BY c.id, c.name
        ORDER BY vacancies_count DESC
        LIMIT :limit OFFSET :offset
    """
    result = db.execute(query, {"limit": limit, "offset": offset}).fetchall()
    return [{"id": r[0], "name": r[1], "vacancies_count": r[2], "avg_salary": r[3]} for r in result]


@app.get("/stats", response_model=StatsResponse)
def get_stats(db: Session = Depends(get_db)):
    total_vacancies = db.query(Vacancy).count()
    total_companies = db.query(Company).count()
    total_skills = db.query(Skill).count()
    
    avg_salary_query = """
        SELECT AVG((salary_min + salary_max)/2) 
        FROM vacancies 
        WHERE salary_min IS NOT NULL AND salary_max IS NOT NULL
    """
    avg_salary = db.execute(avg_salary_query).scalar() or 0
    
    top_skill_query = """
        SELECT s.name, COUNT(vs.vacancy_id) as count
        FROM skills s
        JOIN vacancy_skills vs ON s.id = vs.skill_id
        GROUP BY s.name
        ORDER BY count DESC
        LIMIT 1
    """
    top_skill = db.execute(top_skill_query).first()
    
    return {
        "total_vacancies": total_vacancies,
        "total_companies": total_companies,
        "total_skills": total_skills,
        "avg_salary": round(float(avg_salary), 2),
        "top_skill": top_skill[0] if top_skill else None
    }
