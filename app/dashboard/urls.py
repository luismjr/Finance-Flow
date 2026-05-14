from django.urls import path
from . import views

urlpatterns = [
    path("", views.index, name="index"),
    path("stocks/", views.stocks, name="stocks"),
    path("pipeline/", views.pipeline, name="pipeline"),
    path("quality/", views.quality, name="quality"),
    # JSON API
    path("api/stock-chart/", views.api_stock_chart, name="api_stock_chart"),
    path("api/sector-chart/", views.api_sector_chart, name="api_sector_chart"),
    path("api/volatility-chart/", views.api_volatility_chart, name="api_volatility_chart"),
    path("api/pipeline-chart/", views.api_pipeline_chart, name="api_pipeline_chart"),
    path("api/macro-chart/", views.api_macro_chart, name="api_macro_chart"),
]
