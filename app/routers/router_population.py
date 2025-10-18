import geopandas as gpd
import pandas as pd
import requests
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from loguru import logger
from popframe.method.city_evaluation import CityPopulationScorer
from popframe.method.territory_evaluation import TerritoryEvaluation

from app.common.auth.bearer import verify_bearer_token
from app.common.models.popframe_models.popoframe_dtype.popframe_api_model import (
    PopFrameAPIModel,
)
from app.dependencies import config, pop_frame_model_service, urban_api_gateway

population_router = APIRouter(prefix="/population", tags=["Population Criterion"])


@population_router.post("/get_population_criterion_score", response_model=list[float])
async def get_population_criterion_score_endpoint(
    geojson_data: dict,
    popframe_region_model: PopFrameAPIModel = Depends(
        pop_frame_model_service.get_model
    ),
):
    if geojson_data.get("type") != "FeatureCollection":
        raise HTTPException(
            status_code=400,
            detail="Неверный формат GeoJSON, ожидался FeatureCollection",
        )

    polygon_gdf = gpd.GeoDataFrame.from_features(geojson_data["features"], crs=4326)
    polygon_gdf = polygon_gdf.to_crs(popframe_region_model.region_model.crs)
    if popframe_region_model.region_id in [3138, 3268, 16141]:
        region_mo = await urban_api_gateway.get_mo_for_fed_city_with_population(
            popframe_region_model.region_id
        )
        if len(polygon_gdf) == 1:
            polygon_gdf["hexagon_id"] = 0
        scorer = CityPopulationScorer(region_mo, polygon_gdf)
        return pd.DataFrame(scorer.run())["score"].tolist()
    else:
        evaluation = TerritoryEvaluation(region=popframe_region_model.region_model)
        scores = []
        result = evaluation.population_criterion(territories_gdf=polygon_gdf)
        if result:
            for res in result:
                scores.append(float(res["score"]))
            return scores


async def process_population_criterion(
    popframe_region_model: PopFrameAPIModel, project_scenario_id: int, token: str
):

    scenario_response = requests.get(
        f"{config.get('URBAN_API')}/api/v1/scenarios/{project_scenario_id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    if scenario_response.status_code != 200:
        raise Exception("Ошибка при получении информации по сценарию")

    scenario_data = scenario_response.json()
    project_id = scenario_data.get("project", {}).get("project_id")
    if project_id is None:
        raise Exception("Project ID is missing in scenario data.")

    territory_response = requests.get(
        f"{config.get('URBAN_API')}/api/v1/projects/{project_id}/territory",
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

    evaluation = TerritoryEvaluation(region=popframe_region_model.region_model)
    result = evaluation.population_criterion(territories_gdf=polygon_gdf)

    for res in result:
        indicator_data = {
            "indicator_id": 197,
            "scenario_id": project_scenario_id,
            "territory_id": None,
            "hexagon_id": None,
            "value": float(res["score"]),
            "comment": res["interpretation"],
            "information_source": "modeled PopFrame",
        }

        indicators_response = requests.put(
            f"{config.get('URBAN_API')}/api/v1/scenarios/{project_scenario_id}/indicators_values",
            headers={"Authorization": f"Bearer {token}"},
            json=indicator_data,
        )
        if indicators_response.status_code not in (200, 201):
            logger.exception(
                f"Ошибка при сохранении показателей: {indicators_response.status_code}, "
                f"Тело ответа: {indicators_response.text}"
            )
            raise Exception("Ошибка при сохранении показателей")


@population_router.post("/save_population_criterion")
async def save_population_criterion_endpoint(
    background_tasks: BackgroundTasks,
    popframe_region_model: PopFrameAPIModel = Depends(
        pop_frame_model_service.get_model
    ),
    project_scenario_id: int | None = Query(
        None, description="ID сценария проекта, если имеется"
    ),
    token: str = Depends(verify_bearer_token),
):

    background_tasks.add_task(
        process_population_criterion, popframe_region_model, project_scenario_id, token
    )

    return {
        "message": "Population criterion processing started",
        "status": "processing",
    }
