from dataclasses import dataclass
from typing import List, Optional


@dataclass
class SearchItem:
    id: str
    title: str
    price: int
    thumbnail_url: str
    product_url: str
    merchant_name: str
    merchant_logo_url: str
    source: str
    delivery_text: str = ""
    delivery_days_min: Optional[int] = None
    delivery_days_max: Optional[int] = None


class SearchProvider:
    name: str

    async def search(self, query: str, limit: int) -> List[SearchItem]:
        # pragma: no cover
        raise NotImplementedError
