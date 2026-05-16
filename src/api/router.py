from fastapi import APIRouter

from src.api.routes.auth import router as auth_router
from src.api.routes.documents import router as documents_router
from src.api.routes.institutions import router as institutions_router
from src.api.routes.programs import router as programs_router
from src.api.routes.users import router as users_router

api_router = APIRouter()
api_router.include_router(auth_router)
api_router.include_router(documents_router)
api_router.include_router(institutions_router)
api_router.include_router(programs_router)
api_router.include_router(users_router)
