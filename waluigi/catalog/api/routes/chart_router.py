from fastapi import APIRouter, Depends, Query

from waluigi.commons.utils import _model_dump
from waluigi.catalog.api.schemas import ChartCreateRequest, ChartUpdateRequest
from waluigi.commons.responses import ok, warn, ko
from waluigi.catalog.services.chart_service import ChartService
from waluigi.catalog.config.dependencies import chart_service

chart_router = APIRouter(
    prefix="/datasets",
    tags=["Charts"]
)


@chart_router.get("/{dataset_id:path}/charts",
         summary="List chart definitions for a dataset")
async def list_charts(dataset_id: str, chart_service: ChartService = Depends(chart_service)):
    try:
        return ok(chart_service.list_charts(dataset_id))
    except ValueError as e:
        return ko(str(e), 404)


@chart_router.post("/{dataset_id:path}/charts",
          summary="Add a chart definition")
async def add_chart(dataset_id: str, body: ChartCreateRequest, chart_service: ChartService = Depends(chart_service)):
    try:
        return ok(chart_service.add_chart(
            dataset_id, body.key, body.title, body.spec, body.position))
    except ValueError as e:
        return ko(str(e), 404)


@chart_router.patch("/{dataset_id:path}/charts/{chart_id}",
           summary="Update a chart definition")
async def update_chart(dataset_id: str, chart_id: int, body: ChartUpdateRequest, chart_service: ChartService = Depends(chart_service)):
    try:
        updates = {k: v for k, v in _model_dump(body).items() if v is not None}
        return ok(chart_service.update_chart(dataset_id, chart_id, **updates))
    except ValueError as e:
        return ko(str(e), 404)


@chart_router.delete("/{dataset_id:path}/charts/{chart_id}",
            summary="Delete a chart definition")
async def delete_chart(dataset_id: str, chart_id: int, chart_service: ChartService = Depends(chart_service)):
    try:
        return ok(chart_service.delete_chart(dataset_id, chart_id))
    except ValueError as e:
        return ko(str(e), 404)


@chart_router.get("/{dataset_id:path}/charts/{chart_id}/render",
         summary="Render a chart by ID — returns an ECharts option object")
async def render_chart(dataset_id: str, chart_id: int, version: str = Query(None), chart_service: ChartService = Depends(chart_service)):
    chart = chart_service.get_chart(dataset_id, chart_id)
    if not chart:
        return ko("Chart not found", 404)
    try:
        return ok(chart_service.render(chart, dataset_id, version))
    except ValueError as e:
        return ko(str(e), 404)
    except Exception as e:
        return ko(str(e), 500)


@chart_router.get("/{dataset_id:path}/charts/_render",
         summary="Render a chart by key — returns an ECharts option object")
async def render_chart_by_key(dataset_id: str, key:     str = Query(...), version: str = Query(None), chart_service: ChartService = Depends(chart_service)):
    chart = chart_service.get_chart_by_key(dataset_id, key)
    if not chart:
        return ko("Chart not found", 404)
    try:
        return ok(chart_service.render(chart, dataset_id, version))
    except ValueError as e:
        return ko(str(e), 404)
    except Exception as e:
        return ko(str(e), 500)
