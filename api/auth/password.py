from passlib.context import CryptContext

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def _normalize_password(password: str) -> str:
    if not isinstance(password, str):
        password = str(password)
    pw_bytes = password.encode('utf-8')
    if len(pw_bytes) > 72:
        # bcrypt has a known 72-byte limitation, truncate safely
        password = pw_bytes[:72].decode('utf-8', errors='ignore')
    return password

def verify_password(plain_password: str, hashed_password: str) -> bool:
    normalized = _normalize_password(plain_password)
    return pwd_context.verify(normalized, hashed_password)

def get_password_hash(password: str) -> str:
    normalized = _normalize_password(password)
    return pwd_context.hash(normalized)