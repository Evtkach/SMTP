import os
from datetime import UTC, datetime
from sqlalchemy import Boolean, Column, DateTime, Integer, String, Text, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

# Используем драйвер psycopg (PostgreSQL)
DATABASE_URL = os.getenv("SQLALCHEMY_DATABASE_URL", "postgresql+psycopg://user:password@localhost:5432/dbname")

engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class Subscriber(Base):
    __tablename__ = "subscribers"

    id = Column(Integer, primary_key=True, index=True)
    email = Column(String(255), unique=True, index=True, nullable=False)
    name = Column(String(100), nullable=True)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC))

    def __str__(self):
        return self.email


class Campaign(Base):
    __tablename__ = "campaigns"

    id = Column(Integer, primary_key=True, index=True)
    title = Column(String(200), nullable=False)
    subject = Column(String(255), nullable=False)
    html_body = Column(Text, nullable=False)
    status = Column(String(50), default="Черновик")  # Черновик / Отправлено в Kafka
    created_at = Column(DateTime, default=lambda: datetime.now(UTC))

    def __str__(self):
        return f"{self.title} ({self.status})"


# Автоматически создаем таблицы в PostgreSQL при импорте
Base.metadata.create_all(bind=engine)
