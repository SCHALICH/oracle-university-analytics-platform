# Oracle University Analytics Platform Architecture

## Data Flow

Browser
-> Nginx
-> FastAPI
-> RabbitMQ
-> Analytics Worker
-> Oracle SH.SALES
-> SARIMAX / Seasonal Naive
-> MinIO
-> Dashboard

Redis is used for task state and cache.

Prometheus collects API, worker and Kubernetes metrics.
Grafana visualizes operational telemetry.

## Analytics

The platform supports:

- 3 month forecasting
- 6 month forecasting
- 12 month forecasting

Models:

- SARIMAX
- Seasonal Naive

Validation:

- 6 month holdout
- MAE
- RMSE
- MAPE

The model with the lowest validation MAPE is selected.
