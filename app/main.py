from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
# from app.routers import router_territory, router_population, router_frame, router_agglomeration, router_popframe
# from app.routers import router_landuse, router_recalculate_model
from app.popframe_models.popframe_models_controller import model_calculator_router
from loguru import logger
import sys

logger.remove()
logger.add(
    sys.stdout,
    format="<green>{time:MM-DD HH:mm}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>",
    level="INFO",
    colorize=True
)


app = FastAPI(
    title="PopFrame API",
    description="API for PopFrame service, handling territory evaluation, population criteria, network frame, and land use data.",
    version="3.0.0",
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# # Root endpoint
# @app.get("/", response_model=Dict[str, str])
# def read_root():
#     return {"message": "Welcome to PopFrame Service"}

# Include routers
# app.include_router(router_recalculate_model.region_router)
# app.include_router(router_territory.territory_router)
# app.include_router(router_population.population_router)
# app.include_router(router_frame.network_router)
# app.include_router(router_agglomeration.agglomeration_router)
# app.include_router(router_landuse.landuse_router)
# app.include_router(router_popframe.popframe_router)

app.include_router(model_calculator_router)

# @app.on_event("startup")
# async def startup_event():
#     await get_model.process_models()


