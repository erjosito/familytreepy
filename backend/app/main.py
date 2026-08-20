import os
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from backend.app.routers import persons, relationships, graph, auth_router, geni

APP_VERSION = "0.6.0"

app = FastAPI(
    title="Family Tree API",
    description="API for creating and browsing family trees",
    version=APP_VERSION,
)

# Rate limiting
app.state.limiter = auth_router.limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ORIGINS", "http://localhost:3000").split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth_router.router)
app.include_router(persons.router)
app.include_router(relationships.router)
app.include_router(graph.router)
app.include_router(graph.public_router)
app.include_router(geni.router)


@app.get("/api/health")
def health():
    return {
        "status": "ok",
        "version": APP_VERSION,
        "revision": os.getenv("APP_REVISION", "development"),
        "built_at": os.getenv("APP_BUILD_TIME", "unknown"),
    }


# --- Serve Next.js static export in production ---
_STATIC_DIR = Path(__file__).resolve().parent.parent.parent / "static"

if _STATIC_DIR.is_dir():
    # Catch-all for client-side routes (must be registered before StaticFiles)
    @app.api_route("/{path:path}", methods=["GET"], include_in_schema=False)
    async def _spa_fallback(request: Request, path: str):
        # If the file exists on disk, let StaticFiles handle it
        file = _STATIC_DIR / path
        if file.is_file():
            return FileResponse(file)
        # For directories, try index.html inside them
        index = _STATIC_DIR / path / "index.html"
        if index.is_file():
            return FileResponse(index)
        # Fallback to root index.html (SPA client-side routing)
        return FileResponse(_STATIC_DIR / "index.html")

    app.mount("/", StaticFiles(directory=str(_STATIC_DIR), html=True), name="static")
