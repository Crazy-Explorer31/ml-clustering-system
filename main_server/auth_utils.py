import os
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
from typing import Optional

import bcrypt
from fastapi import Depends, HTTPException, status, Request
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from pydantic import BaseModel

# -----------------------Конфигурация JWT---------------------------

# Загружаем переменные из .env
load_dotenv()

# Глобальные переменные
SECRET_KEY = os.getenv("SECRET_KEY", "change-me-in-production")  # TODO: load from .env
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "admin")
print(ADMIN_USERNAME)
print(ADMIN_USERNAME)
print(ADMIN_USERNAME)
print(ADMIN_USERNAME)
print(ADMIN_USERNAME)
print(ADMIN_USERNAME)
print(ADMIN_USERNAME)

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


# Pydantic модели
class Token(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    username: Optional[str] = None


class User(BaseModel):
    username: str
    email: Optional[str] = None
    full_name: Optional[str] = None
    disabled: Optional[bool] = False


class UserInDB(User):
    hashed_password: str


class UserCreate(User):
    password: str


# Хеширование паролей
def verify_password(plain_password: str, hashed_password: str) -> bool:
    return bcrypt.checkpw(plain_password.encode(), hashed_password.encode())


def get_password_hash(password: str) -> str:
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()


# Работа с Redis
def get_user(redis, username: str) -> Optional[UserInDB]:
    """Получить пользователя из Redis."""
    data = redis.hgetall(f"user:{username}")
    if not data:
        return None
    return UserInDB(
        username=data["username"],
        email=data.get("email"),
        full_name=data.get("full_name"),
        disabled=data.get("disabled") == "True",
        hashed_password=data["hashed_password"],
    )


def create_user(redis, user: UserCreate):
    """Создать пользователя (для регистрации)."""
    existing = redis.exists(f"user:{user.username}")
    if existing:
        return False
    hashed = get_password_hash(user.password)
    mapping = {
        "username": user.username,
        "email": user.email or "",
        "full_name": user.full_name or "",
        "disabled": str(user.disabled),
        "hashed_password": hashed,
    }
    redis.hset(f"user:{user.username}", mapping=mapping)
    return True


def authenticate_user(redis, username: str, password: str):
    user = get_user(redis, username)
    if not user:
        return False
    if not verify_password(password, user.hashed_password):
        return False
    return user


# JWT
def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + (expires_delta or timedelta(minutes=15))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


# Зависимости FastAPI
def get_current_user(request: Request) -> UserInDB:
    """Извлекает текущего пользователя по JWT."""
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Не удалось проверить учётные данные",
        headers={"WWW-Authenticate": "Bearer"},
    )

    # 1. Пробуем получить токен из куки (для браузерных запросов)
    token = request.cookies.get("access_token")

    # 2. Если куки нет — ищем в заголовке Authorization (для API)
    if not token:
        auth_header = request.headers.get("Authorization")
        if auth_header and auth_header.startswith("Bearer "):
            token = auth_header[7:]

    # 3. Если токена нет нигде — ошибка
    if not token:
        raise credentials_exception

    # 4. Декодируем токен
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception

    # 5. Получаем пользователя из Redis
    redis = request.app.state.authorised_users
    user = get_user(redis, username)
    if user is None:
        raise credentials_exception
    return user


def get_current_active_user(
    current_user: User = Depends(get_current_user),
):
    if current_user.disabled:
        raise HTTPException(status_code=400, detail="Неактивный пользователь")
    return current_user


async def get_current_admin_user(current_user: User = Depends(get_current_active_user)):
    print(current_user.username, ADMIN_USERNAME)
    if current_user.username != ADMIN_USERNAME:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="Только для администратора"
        )
    return current_user
