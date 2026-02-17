from sqlalchemy import Column, Integer, String, Text, DateTime, ForeignKey, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime, timezone

Base = declarative_base()

vacancy_skills = Table(
    'vacancy_skills',
    Base.metadata,
    Column('vacancy_id', Integer, ForeignKey('vacancies.id')),
    Column('skill_id', Integer, ForeignKey('skills.id'))
)


class Company(Base):
    __tablename__ = 'companies'

    id = Column(Integer, primary_key=True)
    name = Column(String(200), nullable=False, index=True)
    website = Column(String(500))
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    # Связи
    vacancies = relationship("Vacancy", back_populates="company")

    def __repr__(self):
        return f"<Company {self.name}>"


class Vacancy(Base):
    __tablename__ = 'vacancies'

    id = Column(Integer, primary_key=True)
    title = Column(String(300), nullable=False)
    description = Column(Text)
    salary_min = Column(Integer)  # Минимальная зарплата в рублях
    salary_max = Column(Integer)  # Максимальная зарплата в рублях
    currency = Column(String(10), default='RUB')
    company_id = Column(Integer, ForeignKey('companies.id'))
    published_at = Column(DateTime)
    source = Column(String(50))  # habr, hh, superjob и т.д.
    url = Column(String(500), unique=True)  # Ссылка на вакансию
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    # Связи
    company = relationship("Company", back_populates="vacancies")
    skills = relationship("Skill", secondary=vacancy_skills, back_populates="vacancies")

    def __repr__(self):
        return f"<Vacancy {self.title}>"


class Skill(Base):
    __tablename__ = 'skills'

    id = Column(Integer, primary_key=True)
    name = Column(String(100), unique=True, nullable=False, index=True)
    category = Column(String(50))  # language, framework, tool, etc.
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    # Связи
    vacancies = relationship("Vacancy", secondary=vacancy_skills, back_populates="skills")

    def __repr__(self):
        return f"<Skill {self.name}>"
