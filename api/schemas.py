from pydantic import BaseModel

class TopProduct(BaseModel):
    product: str
    mentions: int
