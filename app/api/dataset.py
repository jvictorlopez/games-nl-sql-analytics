from fastapi import APIRouter
from app.models.schemas import DatasetSummary
from app.services.analytics import dataset_summary

router = APIRouter(prefix="/dataset", tags=["dataset"])

@router.get("/summary", response_model=DatasetSummary)
def summary():
    return dataset_summary()

