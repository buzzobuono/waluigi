import math
import logging
import pandas as pd

from waluigi.catalog.repositories.dataset_repo import DatasetRepository
from waluigi.catalog.repositories.version_repo import VersionRepository
from waluigi.catalog.repositories.source_repo import SourceRepository
from waluigi.catalog.repositories.chart_repo import ChartRepository
from waluigi.sdk.connectors import ConnectorFactory

logger = logging.getLogger("waluigi")


def _safe(v):
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    try:
        return round(float(v), 6)
    except (TypeError, ValueError):
        return str(v)


class ChartService:

    def __init__(self, datasets_repository: DatasetRepository,
                 versions_repository: VersionRepository,
                 sources_repository: SourceRepository,
                 charts_repository: ChartRepository):
        self.datasets_repository = datasets_repository
        self.versions_repository = versions_repository
        self.sources_repository  = sources_repository
        self.charts_repository   = charts_repository

    def render(self, chart: dict, namespace: str, dataset_id: str,
               version: str | None) -> dict:
        dataset = self.datasets_repository.get(namespace, dataset_id)
        if not dataset:
            raise ValueError("Dataset not found")
        browse_path = f"{namespace}/{dataset_id}"
        ver = (self.versions_repository.get(browse_path, version)
               if version else self.versions_repository.get_latest(browse_path))
        if not ver:
            raise ValueError("No committed version available")
        source = self.sources_repository.get(namespace, dataset.source_id)
        if not source:
            raise ValueError(
                f"Source '{dataset.source_id}' not found "
                "— run CatalogCreateSource first"
            )
        connector = ConnectorFactory.get(source.type, source.config)
        df = connector.read(ver.location, dataset.format)
        return {
            "option":    self.build_option(df, chart["spec"]),
            "version":   ver.version,
            "rows":      len(df),
            "is_latest": version is None,
        }

    def list_charts(self, namespace: str, dataset_id: str) -> list:
        if not self.datasets_repository.exists(namespace, dataset_id):
            raise ValueError("Dataset not found")
        return self.charts_repository.list(f"{namespace}/{dataset_id}")

    def get_chart(self, namespace: str, dataset_id: str,
                  chart_id: int) -> dict | None:
        return self.charts_repository.get(f"{namespace}/{dataset_id}", chart_id)

    def get_chart_by_key(self, namespace: str, dataset_id: str,
                         key: str) -> dict | None:
        return self.charts_repository.get_by_key(f"{namespace}/{dataset_id}", key)

    def add_chart(self, namespace: str, dataset_id: str, key: str,
                  title: str, spec: dict, position: int) -> dict:
        if not self.datasets_repository.exists(namespace, dataset_id):
            raise ValueError("Dataset not found")
        return self.charts_repository.add(
            f"{namespace}/{dataset_id}", key, title, spec, position)

    def update_chart(self, namespace: str, dataset_id: str,
                     chart_id: int, **updates) -> dict:
        browse_path = f"{namespace}/{dataset_id}"
        if not self.datasets_repository.exists(namespace, dataset_id):
            raise ValueError("Dataset not found")
        if not self.charts_repository.update(browse_path, chart_id, **updates):
            raise ValueError("Chart not found")
        return self.charts_repository.get(browse_path, chart_id)

    def delete_chart(self, namespace: str, dataset_id: str,
                     chart_id: int) -> dict:
        if not self.datasets_repository.exists(namespace, dataset_id):
            raise ValueError("Dataset not found")
        if not self.charts_repository.delete(f"{namespace}/{dataset_id}", chart_id):
            raise ValueError("Chart not found")
        return {"deleted": chart_id}

    def build_option(self, df: pd.DataFrame, spec: dict) -> dict:
        chart_type  = spec.get("type", "bar")
        x_conf      = spec.get("x", {})
        y_conf      = spec.get("y", {})
        x_field     = x_conf.get("field")
        y_field     = y_conf.get("field")
        x_label     = x_conf.get("label", x_field or "")
        y_label     = y_conf.get("label", y_field or "")
        agg         = y_conf.get("agg", "sum")
        color_field = spec.get("color")
        limit       = int(spec.get("limit", 200))
        sort        = x_conf.get("sort")  # "asc", "desc", or None (data order)

        base = {
            "tooltip": {},
            "toolbox": {"feature": {"saveAsImage": {}, "dataZoom": {}}},
            "grid":    {"containLabel": True},
        }

        if chart_type == "pie":
            grouped = (df.groupby(x_field)[y_field].agg(agg)
                       .reset_index().head(limit))
            data = [{"name": str(r[x_field]), "value": _safe(r[y_field])}
                    for _, r in grouped.iterrows()]
            return {**base,
                    "tooltip": {"trigger": "item", "formatter": "{b}: {c} ({d}%)"},
                    "legend":  {"orient": "vertical", "left": "left"},
                    "series":  [{"type": "pie", "data": data,
                                 "radius": "60%", "label": {"formatter": "{b}\n{d}%"}}]}

        if chart_type == "histogram":
            col    = df[x_field].dropna()
            bins   = int(spec.get("bins", 20))
            cuts   = pd.cut(col, bins=bins)
            counts = cuts.value_counts().sort_index()
            cats   = [str(i) for i in counts.index]
            vals   = [int(v) for v in counts.values]
            return {**base,
                    "tooltip": {"trigger": "axis"},
                    "xAxis":   {"type": "category", "data": cats,
                                "name": x_label, "axisLabel": {"rotate": 30}},
                    "yAxis":   {"type": "value", "name": "Count"},
                    "series":  [{"type": "bar", "data": vals, "barWidth": "99%"}]}

        if chart_type == "scatter":
            data = (df[[x_field, y_field]].dropna().head(limit)
                    .apply(lambda r: [_safe(r[x_field]), _safe(r[y_field])], axis=1)
                    .tolist())
            return {**base,
                    "tooltip": {"trigger": "item"},
                    "xAxis":   {"type": "value", "name": x_label},
                    "yAxis":   {"type": "value", "name": y_label},
                    "series":  [{"type": "scatter", "data": data, "symbolSize": 8}]}

        if chart_type == "radar":
            axes     = spec.get("axes", [])
            group_by = spec.get("group_by") or color_field
            if len(axes) < 3:
                raise ValueError("radar requires at least 3 axes to form a polygon")
            ax_fields = [a["field"] for a in axes]
            ax_labels = [a.get("label", a["field"]) for a in axes]
            if group_by:
                grouped   = df.groupby(group_by)[ax_fields].agg(
                    agg if agg != "count" else "sum")
                maxes     = [grouped[f].max() for f in ax_fields]
                indicator = [
                    {"name": lbl, "max": a.get("max") or (_safe(mx) * 1.2 if mx else 1)}
                    for a, lbl, mx in zip(axes, ax_labels, maxes)
                ]
                series_data = [
                    {"name": str(grp),
                     "value": [_safe(grouped.loc[grp, f]) for f in ax_fields]}
                    for grp in grouped.index
                ]
                legend = {"data": [str(g) for g in grouped.index]}
            else:
                agged     = df[ax_fields].agg(agg if agg != "count" else "sum")
                vals      = [_safe(agged[f]) for f in ax_fields]
                maxes     = [abs(v) * 1.2 if v else 1 for v in vals]
                indicator = [
                    {"name": lbl, "max": a.get("max") or mx}
                    for a, lbl, mx in zip(axes, ax_labels, maxes)
                ]
                series_data = [{"name": "Total", "value": vals}]
                legend      = {}
            opt = {**base,
                   "tooltip": {"trigger": "item"},
                   "radar":   {"indicator": indicator, "shape": "polygon"},
                   "series":  [{"type": "radar", "data": series_data,
                                "areaStyle": {"opacity": 0.2}}]}
            if legend:
                opt["legend"] = legend
            return opt

        if color_field:
            if agg == "count":
                agged   = (df.groupby([x_field, color_field])
                           .size().reset_index(name=y_field or "_count"))
                y_field = y_field or "_count"
            else:
                agged = (df.groupby([x_field, color_field])[y_field]
                         .agg(agg).reset_index())
            pivot  = agged.pivot(index=x_field, columns=color_field, values=y_field).fillna(0)
            if sort == "asc":
                pivot = pivot.sort_index(ascending=True)
            elif sort == "desc":
                pivot = pivot.sort_index(ascending=False)
            pivot  = pivot.head(limit)
            cats   = [str(c) for c in pivot.index]
            series = [{"name": str(col), "type": chart_type,
                       "data": [_safe(v) for v in pivot[col]]}
                      for col in pivot.columns]
            legend = {"data": [str(c) for c in pivot.columns]}
        else:
            if agg == "count":
                grouped         = df[x_field].value_counts().reset_index()
                grouped.columns = [x_field, "_count"]
                if sort == "asc":
                    grouped = grouped.sort_values(x_field, ascending=True)
                elif sort == "desc":
                    grouped = grouped.sort_values(x_field, ascending=False)
                grouped = grouped.head(limit)
                cats = [str(v) for v in grouped[x_field]]
                vals = [int(v) for v in grouped["_count"]]
            else:
                grouped = df.groupby(x_field)[y_field].agg(agg).reset_index()
                if sort == "asc":
                    grouped = grouped.sort_values(x_field, ascending=True)
                elif sort == "desc":
                    grouped = grouped.sort_values(x_field, ascending=False)
                grouped = grouped.head(limit)
                cats = [str(v) for v in grouped[x_field]]
                vals = [_safe(v) for v in grouped[y_field]]
            series = [{"type": chart_type, "data": vals, "name": y_label}]
            legend = {}

        opt = {**base,
               "tooltip": {"trigger": "axis"},
               "xAxis":   {"type": "category", "data": cats,
                           "name": x_label, "axisLabel": {"rotate": 30}},
               "yAxis":   {"type": "value", "name": y_label},
               "series":  series}
        if legend:
            opt["legend"] = legend
        return opt
