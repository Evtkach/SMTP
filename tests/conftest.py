import os
os.environ["SQLALCHEMY_DATABASE_URL"] = "sqlite:///:memory:"

import pytest
from sqlalchemy.pool import StaticPool
import models

# Перенастраиваем общий engine на StaticPool, чтобы SQLite в памяти
# сохранял таблицы между разными потоками FastAPI TestClient
models.engine.dispose()
from sqlalchemy import create_engine
test_engine = create_engine(
    "sqlite:///:memory:",
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)
models.engine = test_engine
models.SessionLocal.configure(bind=test_engine)

import admin_app


@pytest.fixture(autouse=True)
def setup_test_db():
    models.Base.metadata.create_all(bind=test_engine)
    yield
    models.Base.metadata.drop_all(bind=test_engine)


@pytest.fixture
def db_session():
    session = models.SessionLocal()
    try:
        yield session
    finally:
        session.close()
