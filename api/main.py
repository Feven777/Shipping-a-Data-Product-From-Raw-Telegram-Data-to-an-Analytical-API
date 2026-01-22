from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List
import logging
import traceback

from api.database import get_db
from api.schemas import TopProduct

logger = logging.getLogger("uvicorn.error")

app = FastAPI(
    title="Medical Telegram Analytical API",
    description="Exposes analytical insights from Telegram medical channels",
    version="1.0.0",
    debug=True
)

@app.get("/")
def root():
    return {"message": "API is running"}

@app.get("/health")
def health_check(db: Session = Depends(get_db)):
    return {
        "status": "ok",
        "database": "connected"
    }

@app.get(
    "/api/reports/top-products",
    response_model=List[TopProduct],
    summary="Top mentioned products across all channels"
)
def top_products(
    limit: int = 10,
    db: Session = Depends(get_db)
):
    logger.info("top-products called")

    query = text("""
        SELECT
            lower(word) AS product,
            count(*) AS mentions
        FROM (
            SELECT
                unnest(
                    regexp_split_to_array(message_text, '\\s+')
                ) AS word
            FROM staging.fct_messages
            WHERE message_text IS NOT NULL
        ) t
        WHERE length(word) > 3
        GROUP BY product
        ORDER BY mentions DESC
        LIMIT :limit
    """)

    try:
        result = db.execute(query, {"limit": limit}).fetchall()
        logger.info("Query executed successfully")
    except Exception as e:
        logger.exception("Query failed")
        traceback.print_exc()
        raise e

    try:
        data = [
            {"product": row.product, "mentions": row.mentions}
            for row in result
        ]
        logger.info("Response prepared")
    except Exception as e:
        logger.exception("Response conversion failed")
        traceback.print_exc()
        raise e

    return data
