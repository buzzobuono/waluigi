import math
import logging
import pandas as pd

from waluigi.catalog.db import CatalogDB
from waluigi.sdk.connectors import ConnectorFactory

logger = logging.getLogger("waluigi")


def _safe(v):
    """Convert a value to a JSON-serialisable scalar."""
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    try:
        return round(float(v), 6)
    except (TypeError, ValueError):
        return str(v)


class ChartService:

    def __init__(self, db: CatalogDB):
        self.db = db

    # ── Public API ────────────────────────────────────────────────────────────

    def render(self, chart: dict, dataset_id: str,
               version: str | None) -> dict:
        """Load data from storage and build an ECharts option dict.

        Raises ValueError for any not-found condition so callers can map it
        to an appropriate HTTP status without importing HTTP primitives here.
        """
        dataset = self.db.get_dataset(dataset_id)
        if not dataset:
            raise ValueError("Dataset not found")

        ver = (self.db.get_version(dataset_id, version)
               if version else self.db.get_latest_version(dataset_id))
        if not ver:
            raise ValueError("No committed version available")

        source = self.db.get_source(dataset["source_id"])
        if not source:
            raise ValueError(
                f"Source '{dataset['source_id']}' not found "
                "— run CatalogCreateSource first"
            )

        connector = ConnectorFactory.get(source["type"], source["config"])
        df = connector.read(ver["location"], dataset["format"])

        return {
            "option":    self.build_option(df, chart["spec"]),
            "version":   ver["version"],
            "rows":      len(df),
            "is_latest": version is None,
        }

    # ── Chart CRUD ────────────────────────────────────────────────────────────

    def list_charts(self, dataset_id: str) -> list:
        if not self.db.exists_dataset(dataset_id):
            raise ValueError("Dataset not found")
        return self.db.list_charts(dataset_id)

    def get_chart(self, dataset_id: str, chart_id: int) -> dict | None:
        return self.db.get_chart(dataset_id, chart_id)

    def get_chart_by_key(self, dataset_id: str, key: str) -> dict | None:
        return self.db.get_chart_by_key(dataset_id, key)

    def add_chart(self, dataset_id: str, key: str, title: str,
                  spec: dict, position: int) -> dict:
        if not self.db.exists_dataset(dataset_id):
            raise ValueError("Dataset not found")
        return self.db.add_chart(dataset_id, key, title, spec, position)

    def update_chart(self, dataset_id: str, chart_id: int, **updates) -> dict:
        """Raises ValueError if dataset or chart not found."""
        if not self.db.exists_dataset(dataset_id):
            raise ValueError("Dataset not found")
        if not self.db.update_chart(dataset_id, chart_id, **updates):
            raise ValueError("Chart not found")
        return self.db.get_chart(dataset_id, chart_id)

    def delete_chart(self, dataset_id: str, chart_id: int) -> dict:
        """Raises ValueError if dataset or chart not found."""
        if not self.db.exists_dataset(dataset_id):
            raise ValueError("Dataset not found")
        if not self.db.delete_chart(dataset_id, chart_id):
            raise ValueError("Chart not found")
        return {"deleted": chart_id}

    # ── Render ────────────────────────────────────────────────────────────────

    def build_option(self, df: pd.DataFrame, spec: dict) -> dict:
        """Build an ECharts option dict from a DataFrame and a chart spec."""
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

        base = {
            "tooltip": {},
            "toolbox": {"feature": {"saveAsImage": {}, "dataZoom": {}}},
            "grid":    {"containLabel": True},
        }

        # ── PIE ──────────────────────────────────────────────────────────────
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

        # ── HISTOGRAM ────────────────────────────────────────────────────────
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

        # ── SCATTER ──────────────────────────────────────────────────────────
        if chart_type == "scatter":
            data = (df[[x_field, y_field]].dropna().head(limit)
                    .apply(lambda r: [_safe(r[x_field]), _safe(r[y_field])], axis=1)
                    .tolist())
            return {**base,
                    "tooltip": {"trigger": "item"},
                    "xAxis":   {"type": "value", "name": x_label},
                    "yAxis":   {"type": "value", "name": y_label},
                    "series":  [{"type": "scatter", "data": data, "symbolSize": 8}]}

        # ── RADAR ────────────────────────────────────────────────────────────
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

        # ── BAR / LINE ───────────────────────────────────────────────────────
        if color_field:
            if agg == "count":
                agged   = (df.groupby([x_field, color_field])
                           .size().reset_index(name=y_field or "_count"))
                y_field = y_field or "_count"
            else:
                agged = (df.groupby([x_field, color_field])[y_field]
                         .agg(agg).reset_index())
            pivot  = (agged.pivot(index=x_field, columns=color_field, values=y_field)
                      .fillna(0).head(limit))
            cats   = [str(c) for c in pivot.index]
            series = [{"name": str(col), "type": chart_type,
                       "data": [_safe(v) for v in pivot[col]]}
                      for col in pivot.columns]
            legend = {"data": [str(c) for c in pivot.columns]}
        else:
            if agg == "count":
                grouped         = df[x_field].value_counts().reset_index().head(limit)
                grouped.columns = [x_field, "_count"]
                cats = [str(v) for v in grouped[x_field]]
                vals = [int(v) for v in grouped["_count"]]
            else:
                grouped = (df.groupby(x_field)[y_field]
                           .agg(agg).reset_index().head(limit))
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
