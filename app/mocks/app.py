from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.mocks.coursera import router as coursera_router
from app.mocks.datacamp import router as datacamp_router
from app.mocks.fams import router as fams_router
from app.mocks.harvard import router as harvard_router
from app.mocks.levelup import router as levelup_router
from app.mocks.linkedin import router as linkedin_router
from app.mocks.settings import get_mock_settings
from app.mocks.skillup import router as skillup_router


@asynccontextmanager
async def lifespan(_application: FastAPI) -> AsyncGenerator[None, None]:
    get_mock_settings().validate_runtime()
    yield


app = FastAPI(title="Mock Learning Vendor Hub", lifespan=lifespan)

app.include_router(levelup_router, prefix="/levelup")
app.include_router(skillup_router, prefix="/skillup")
app.include_router(datacamp_router, prefix="/datacamp")
app.include_router(coursera_router, prefix="/coursera")
app.include_router(linkedin_router, prefix="/linkedin")
app.include_router(harvard_router, prefix="/harvard")
app.include_router(fams_router, prefix="/fams")


@app.get("/", include_in_schema=False)
async def index() -> dict[str, str]:
    return {
        "service": "Mock Learning Vendor Hub",
        "docs": "/docs",
        "levelup": "/levelup",
        "skillup": "/skillup",
        "datacamp": "/datacamp",
        "coursera": "/coursera",
        "linkedin": "/linkedin",
        "harvard": "/harvard",
        "fams": "/fams",
    }
