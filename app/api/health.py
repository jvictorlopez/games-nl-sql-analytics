from fastapi import APIRouter
from app.core.config import get_settings
from app.models.schemas import HealthOut

router = APIRouter(tags=["health"])

@router.get("/health", response_model=HealthOut)
def health():
    s = get_settings()
    return HealthOut(status="ok", version=s.APP_VERSION)

