import sys

import geopandas as gpd
import requests
from fastapi import APIRouter, BackgroundTasks, Depends, Query
from loguru import logger
from popframe.method.territory_evaluation import TerritoryEvaluation

from app.common.models.popframe_models.popframe_models_service import \
    pop_frame_model_service
from app.common.models.popframe_models.popoframe_dtype.popframe_api_model import \
    PopFrameAPIModel
from app.dependencies import config
from app.utils.auth import verify_token

popframe_router = APIRouter(prefix="/popframe", tags=["PopFrame Evaluation"])

logger.remove()
logger.add(
    sys.stdout,
    format="<green>{time:MM-DD HH:mm}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>",
    level="INFO",
    colorize=True,
)


async def process_combined_evaluation(
    popframe_region_model: PopFrameAPIModel, project_scenario_id: int, token: str
):
    try:
        # Общая часть — получение project_id и геометрии территории
        scenario_response = requests.get(
            f"{config.get('URBAN_API')}/scenarios/{project_scenario_id}",
            headers={"Authorization": f"Bearer {token}"},
        )
        if scenario_response.status_code != 200:
            raise Exception("Ошибка при получении информации по сценарию")

        scenario_data = scenario_response.json()
        project_id = scenario_data.get("project", {}).get("project_id")
        if project_id is None:
            raise Exception("Project ID is missing in scenario data.")

        territory_response = requests.get(
            f"{config.get('URBAN_API')}/projects/{project_id}/territory",
            headers={"Authorization": f"Bearer {token}"},
        )
        if territory_response.status_code != 200:
            raise Exception("Ошибка при получении геометрии территории")

        territory_data = territory_response.json()
        territory_geometry = territory_data["geometry"]
        territory_feature = {
            "type": "Feature",
            "geometry": territory_geometry,
            "properties": {},
        }
        polygon_gdf = gpd.GeoDataFrame.from_features([territory_feature], crs=4326)
        polygon_gdf = polygon_gdf.to_crs(popframe_region_model.region_model.crs)

        # Оценка территории
        evaluation = TerritoryEvaluation(region=popframe_region_model.region_model)

        # Выполнение первой оценки
        location_results = evaluation.evaluate_territory_location(
            territories_gdf=polygon_gdf
        )
        for res in location_results:
            closest_settlements = [
                res["closest_settlement"],
                res["closest_settlement1"],
                res["closest_settlement2"],
            ]
            settlements = [
                settlement for settlement in closest_settlements if settlement
            ]
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
                "properties": {"attribute_name": "Оценка по каркасу расселения"},
            }

            indicators_response = requests.put(
                f"{config.get('URBAN_API')}/scenarios/indicators_values",
                headers={"Authorization": f"Bearer {token}"},
                json=indicator_data,
            )
            if indicators_response.status_code not in (200, 201):
                logger.exception(
                    f"Ошибка при сохранении показателей (локация): {indicators_response.status_code}, "
                    f"Тело ответа: {indicators_response.text}"
                )
                raise Exception("Ошибка при сохранении показателей (локация)")

        # Выполнение второй оценки
        population_results = evaluation.population_criterion(
            territories_gdf=polygon_gdf
        )
        for res in population_results:
            indicator_data = {
                "indicator_id": 197,
                "scenario_id": project_scenario_id,
                "territory_id": None,
                "hexagon_id": None,
                "value": float(res["score"]),
                "comment": res["interpretation"],
                "information_source": "modeled PopFrame",
                "properties": {"attribute_name": "Население"},
            }

            indicators_response = requests.put(
                f"{config.get('URBAN_API')}/scenarios/indicators_values",
                headers={"Authorization": f"Bearer {token}"},
                json=indicator_data,
            )
            if indicators_response.status_code not in (200, 201):
                logger.exception(
                    f"Ошибка при сохранении показателей (население): {indicators_response.status_code}, "
                    f"Тело ответа: {indicators_response.text}"
                )
                raise Exception("Ошибка при сохранении показателей (население)")

    except Exception as e:
        logger.exception(f"Ошибка при комбинированной обработке: {e}")


@popframe_router.put("/save_popframe_evaluation")
async def save_popframe_evaluation_endpoint(
    background_tasks: BackgroundTasks,
    popframe_region_model: PopFrameAPIModel = Depends(
        pop_frame_model_service.get_model
    ),
    project_scenario_id: int | None = Query(
        None, description="ID сценария проекта, если имеется"
    ),
    token: str = Depends(verify_token),
):
    # Добавляем фоновую задачу для комбинированной обработки
    background_tasks.add_task(
        process_combined_evaluation, popframe_region_model, project_scenario_id, token
    )

    return {"message": "PopFrame evaluation processing started", "status": "processing"}
