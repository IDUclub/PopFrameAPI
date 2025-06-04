import sys

import geopandas as gpd
import requests
from fastapi import (APIRouter, BackgroundTasks, Depends, Header,
                     HTTPException, Query, Request)
from loguru import logger
from popframe.method.territory_evaluation import TerritoryEvaluation
from pydantic_geojson import PolygonModel

from app.common.models.popframe_models.popframe_models_service import \
    pop_frame_model_service
from app.common.models.popframe_models.popoframe_dtype.popframe_api_model import \
    PopFrameAPIModel
from app.dependences import config
from app.models.models import EvaluateTerritoryLocationResult
from app.utils.auth import verify_token

territory_router = APIRouter(prefix="/territory", tags=["Territory Evaluation"])

logger.remove()
logger.add(
    sys.stdout,
    format="<green>{time:MM-DD HH:mm}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>",
    level="INFO",
    colorize=True,
)


@territory_router.post(
    "/evaluate_location_test", response_model=list[EvaluateTerritoryLocationResult]
)
async def evaluate_territory_location_endpoint(
    polygon: PolygonModel,
    popframe_region_model: PopFrameAPIModel = Depends(
        pop_frame_model_service.get_model
    ),
    project_scenario_id: int | None = Query(
        None, description="ID сценария проекта, если имеется"
    ),
    token: str = Depends(verify_token),  # Добавляем токен для аутентификации
):
    try:
        evaluation = TerritoryEvaluation(region=popframe_region_model.region_model)
        polygon_feature = {
            "type": "Feature",
            "geometry": polygon.model_dump(),
            "properties": {},
        }
        polygon_gdf = gpd.GeoDataFrame.from_features([polygon_feature], crs=4326)
        polygon_gdf = polygon_gdf.to_crs(popframe_region_model.region_model.crs)
        result = evaluation.evaluate_territory_location(territories_gdf=polygon_gdf)
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


async def process_evaluation(
    popframe_region_model: PopFrameAPIModel, project_scenario_id: int, token: str
):
    try:
        # Getting project_id and additional information based on scenario_id
        scenario_response = requests.get(
            f"{config.get('URBAN_API')}/scenarios/{project_scenario_id}",
            headers={"Authorization": f"Bearer {token}"},
        )
        if scenario_response.status_code != 200:
            raise Exception("Error retrieving scenario information")

        scenario_data = scenario_response.json()
        project_id = scenario_data.get("project", {}).get("project_id")
        if project_id is None:
            raise Exception("Project ID is missing in scenario data.")

        # Retrieving territory geometry
        territory_response = requests.get(
            f"{config.get('URBAN_API')}/projects/{project_id}/territory",
            headers={"Authorization": f"Bearer {token}"},
        )
        if territory_response.status_code != 200:
            raise Exception("Error retrieving territory geometry")

        # Extracting only the polygon geometry
        territory_data = territory_response.json()
        territory_geometry = territory_data["geometry"]

        # Converting the territory geometry to GeoDataFrame
        territory_feature = {
            "type": "Feature",
            "geometry": territory_geometry,
            "properties": {},
        }
        polygon_gdf = gpd.GeoDataFrame.from_features([territory_feature], crs=4326)
        polygon_gdf = polygon_gdf.to_crs(popframe_region_model.region_model.crs)

        # Territory evaluation
        evaluation = TerritoryEvaluation(region=popframe_region_model.region_model)
        result = evaluation.evaluate_territory_location(territories_gdf=polygon_gdf)

        # Saving the evaluation to the database
        for res in result:
            closest_settlements = [
                res["closest_settlement"],
                res["closest_settlement1"],
                res["closest_settlement2"],
            ]
            settlements = [
                settlement for settlement in closest_settlements if settlement
            ]

            # Создаем строку интерпретации
            interpretation = f'{res["interpretation"]}'
            if settlements:
                interpretation += (
                    f' (Ближайший населенный пункт: {", ".join(settlements)}).'
                )

            indicator_data = {
                "indicator_id": 195,
                "scenario_id": project_scenario_id,
                "territory_id": None,
                "hexagon_id": None,
                "value": float(res["score"]),
                "comment": interpretation,
                "information_source": "modeled PopFrame",
            }

            indicators_response = requests.post(
                f"{config.get('URBAN_API')}/scenarios/indicators_values",
                headers={"Authorization": f"Bearer {token}"},
                json=indicator_data,
            )
            if indicators_response.status_code not in (200, 201):
                logger.exception(
                    f"Error saving indicators: {indicators_response.status_code}, "
                    f"Response body: {indicators_response.text}"
                )
                raise Exception("Error saving indicators")
    except Exception as e:
        logger.exception(f"Error during saving indicators {e.__str__()}")


@territory_router.post("/save_evaluate_location")
async def save_evaluate_location_endpoint(
    background_tasks: BackgroundTasks,
    popframe_region_model: PopFrameAPIModel = Depends(
        pop_frame_model_service.get_model
    ),
    project_scenario_id: int | None = Query(
        None, description="Project scenario ID, if available"
    ),
    token: str = Depends(verify_token),  # Добавляем токен для аутентификации
):
    # Добавляем фоновую задачу
    background_tasks.add_task(
        process_evaluation, popframe_region_model, project_scenario_id, token
    )

    return {
        "message": "Population criterion processing started",
        "status": "processing",
    }
