
"""REST: повнотекстовий пошук препаратів."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from app.application.dto.search import SearchQuery, SearchResult
from app.application.ports.inbound.search_medicines import SearchMedicines
from app.domain.errors import SearchUnavailableError


class SearchController:
    def __init__(self, search_medicines: SearchMedicines) -> None:
        self._search = search_medicines

        self.router = APIRouter(prefix="/api", tags=["search"])

        self.router.add_api_route("/search", self.search, methods=["GET"], response_model=SearchResult)

    async def search(self, q: str = Query(..., min_length=1, description="Текст пошукового запиту"),
        is_prescription: bool | None = None,
        price_max: float | None = Query(None, ge=0),
        limit: int = Query(20, ge=1, le=100)
    ) -> SearchResult:
        """Виконання повнотекстового пошуку препаратів."""

        filters = {}

        if is_prescription is not None:
            filters["is_prescription"] = is_prescription

        if price_max is not None:
            filters["price_max"] = price_max

        query = SearchQuery(raw=q, filters=filters, limit=limit)

        try:
            return await self._search.handle(query)
        except (SearchUnavailableError, TimeoutError) as exc:
            raise HTTPException(status_code=503,
                                detail="Повнотекстовий пошук тимчасово недоступний") from exc