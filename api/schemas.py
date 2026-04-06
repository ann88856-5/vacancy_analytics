from pydantic import BaseModel
from typing import Optional, List


class VacancyResponse(BaseModel):
    id: int
    title: str
    description: Optional[str] = None
    salary_min: Optional[int] = None
    salary_max: Optional[int] = None
    currency: Optional[str] = None
    company_name: str
    source: str
    url: Optional[str] = None


class SkillResponse(BaseModel):
    id: int
    name: str
    category: Optional[str] = None
    count: Optional[int] = None


class CompanyResponse(BaseModel):
    id: int
    name: str
    vacancies_count: Optional[int] = None
    avg_salary: Optional[float] = None


class StatsResponse(BaseModel):
    total_vacancies: int
    total_companies: int
    total_skills: int
    avg_salary: float
    top_skill: Optional[str] = None
    