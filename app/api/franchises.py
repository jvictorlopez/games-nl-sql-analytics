from fastapi import APIRouter, HTTPException
import pandas as pd
from typing import List
from app.models.schemas import FranchiseStats, RankingItem
from app.services.datastore import get_datastore
from app.services.analytics import _prepare_items  # internal use OK
from app.utils.franchise import infer_franchise

router = APIRouter(prefix="/franchises", tags=["franchises"])

@router.get("/{slug}", response_model=FranchiseStats)
def franchise(slug: str):
    df = get_datastore().get_df()
    rows = df[df["Name"].apply(lambda n: infer_franchise(str(n)) == slug)]
    if rows.empty:
        raise HTTPException(status_code=404, detail=f"Franchise '{slug}' not found")
    data = _prepare_items(rows, include_combo=True)
    total_sales = float(data["Global_Sales"].fillna(0).sum())
    avg_c = float(data["Critic_Score"].mean()) if "Critic_Score" in data else None
    avg_u = float(data["User_Score"].mean()) if "User_Score" in data else None
    top = data.sort_values(by=["score_combo","Global_Sales"], ascending=[False,False]).head(10)
    items: List[RankingItem] = []
    for _, r in top.iterrows():
        items.append(RankingItem(
            name=r["Name"], platform=r["Platform"],
            year=int(r["Year_of_Release"]) if pd.notna(r["Year_of_Release"]) else None,
            genre=r["Genre"], publisher=r["Publisher"],
            global_sales=r["Global_Sales"], critic_score=r["Critic_Score"], user_score=r["User_Score"],
            score_combo=r.get("score_combo"), critic_count=r["Critic_Count"], user_count=r["User_Count"]
        ))
    return FranchiseStats(
        slug=slug, total_titles=int(len(data)), total_global_sales=total_sales,
        avg_critic=avg_c, avg_user=avg_u, top_entries=items
    )

