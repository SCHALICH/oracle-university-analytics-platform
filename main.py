import time
from collections import defaultdict
from fastapi.responses import PlainTextResponse
"""Oracle University Analytics REST API."""

import json
import os
from io import BytesIO
from typing import Literal
from uuid import uuid4

import pika
import jwt
import oracledb
from fastapi.responses import HTMLResponse
from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jwt import PyJWKClient
from jwt.exceptions import PyJWTError
from minio import Minio
from minio.error import S3Error
from pika.exceptions import AMQPError
from pydantic import BaseModel, Field
from redis import Redis
from redis.exceptions import RedisError


app = FastAPI(
    title="Oracle University Analytics API",
    version="1.0.0",
    description="University analytics platform service endpoints.",
)


# ============================================================
# University Application Metrics V1
# ============================================================

_METRIC_REQUEST_COUNT = defaultdict(int)
_METRIC_REQUEST_DURATION_SUM = defaultdict(float)
_METRIC_REQUEST_DURATION_COUNT = defaultdict(int)
_METRIC_TASK_CREATE_COUNT = defaultdict(int)


def _metric_path(path: str) -> str:
    if path.startswith("/api/v1/tasks/") and path != "/api/v1/tasks":
        return "/api/v1/tasks/{message_id}"

    if path.startswith("/api/v1/reports/") and path != "/api/v1/reports":
        return "/api/v1/reports/{message_id}"

    return path


@app.middleware("http")
async def university_metrics_middleware(request, call_next):
    started = time.perf_counter()
    response = None

    try:
        response = await call_next(request)
        return response

    finally:
        elapsed = time.perf_counter() - started

        method = request.method
        path = _metric_path(request.url.path)
        status = getattr(response, "status_code", 500)

        _METRIC_REQUEST_COUNT[(method, path, str(status))] += 1
        _METRIC_REQUEST_DURATION_SUM[(method, path)] += elapsed
        _METRIC_REQUEST_DURATION_COUNT[(method, path)] += 1

        if (
            method == "POST"
            and path == "/api/v1/tasks"
            and int(status) < 400
        ):
            _METRIC_TASK_CREATE_COUNT["sales-forecast"] += 1


@app.get("/metrics", response_class=PlainTextResponse)
def application_metrics():
    lines = [
        "# HELP university_api_requests_total Total FastAPI HTTP requests.",
        "# TYPE university_api_requests_total counter",
    ]

    for (method, path, status), value in sorted(_METRIC_REQUEST_COUNT.items()):
        lines.append(
            'university_api_requests_total'
            f'{{method="{method}",path="{path}",status="{status}"}} {value}'
        )

    lines += [
        "# HELP university_api_request_duration_seconds_sum Total HTTP request duration.",
        "# TYPE university_api_request_duration_seconds_sum counter",
    ]

    for (method, path), value in sorted(_METRIC_REQUEST_DURATION_SUM.items()):
        lines.append(
            'university_api_request_duration_seconds_sum'
            f'{{method="{method}",path="{path}"}} {value:.6f}'
        )

    lines += [
        "# HELP university_api_request_duration_seconds_count Requests included in duration metric.",
        "# TYPE university_api_request_duration_seconds_count counter",
    ]

    for (method, path), value in sorted(_METRIC_REQUEST_DURATION_COUNT.items()):
        lines.append(
            'university_api_request_duration_seconds_count'
            f'{{method="{method}",path="{path}"}} {value}'
        )

    lines += [
        "# HELP university_tasks_created_total Forecast tasks created through API.",
        "# TYPE university_tasks_created_total counter",
    ]

    for task_type, value in sorted(_METRIC_TASK_CREATE_COUNT.items()):
        lines.append(
            'university_tasks_created_total'
            f'{{task_type="{task_type}"}} {value}'
        )

    lines += [
        "# HELP university_api_up University FastAPI process status.",
        "# TYPE university_api_up gauge",
        "university_api_up 1",
    ]

    return "\n".join(lines) + "\n"


redis_client = Redis.from_url(
    os.getenv("REDIS_URL", "redis://redis:6379/0"),
    decode_responses=True,
    socket_connect_timeout=2,
    socket_timeout=2,
)

rabbitmq_url = os.getenv(
    "RABBITMQ_URL",
    "amqp://guest:guest@rabbitmq:5672/%2F",
)
rabbitmq_queue = os.getenv("RABBITMQ_QUEUE", "university.tasks")
minio_bucket = os.getenv("MINIO_BUCKET", "university-reports")
minio_client = Minio(
    os.getenv("MINIO_ENDPOINT", "minio:9000"),
    access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
    secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
)
keycloak_issuer = os.getenv(
    "KEYCLOAK_ISSUER",
    "http://127.0.0.1:8180/realms/university",
)
keycloak_jwks_url = os.getenv(
    "KEYCLOAK_JWKS_URL",
    f"{keycloak_issuer}/protocol/openid-connect/certs",
)
keycloak_client_id = os.getenv("KEYCLOAK_CLIENT_ID", "university-api")
keycloak_allowed_client_ids = {
    item.strip()
    for item in os.getenv(
        "KEYCLOAK_ALLOWED_CLIENT_IDS",
        "university-api,university-dashboard",
    ).split(",")
    if item.strip()
}
jwks_client = PyJWKClient(keycloak_jwks_url, cache_keys=True)
bearer_scheme = HTTPBearer(auto_error=False)


def required_env(name: str) -> str:
    """Return a required environment setting without exposing its value."""
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Required environment variable is missing: {name}")
    return value


class TaskRequest(BaseModel):
    """Describe a background analytics task."""

    task_type: Literal[
        "grade-report",
        "sales-forecast",
        "student-risk-analysis",
    ]
    payload: dict[str, object] = Field(default_factory=dict)


class ReportRequest(BaseModel):
    """Describe a text report stored in object storage."""

    filename: str = Field(
        min_length=1,
        max_length=128,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]*$",
    )
    content: str = Field(min_length=1, max_length=1_000_000)
    content_type: str = Field(default="text/plain", max_length=100)


def decode_access_token(token: str) -> dict[str, object]:
    """Validate a Keycloak access token and return its claims."""
    signing_key = jwks_client.get_signing_key_from_jwt(token)
    claims = jwt.decode(
        token,
        signing_key.key,
        algorithms=["RS256"],
        issuer=keycloak_issuer,
        options={"verify_aud": False},
    )
    if claims.get("azp") not in keycloak_allowed_client_ids:
        raise jwt.InvalidAudienceError("Unexpected authorized party")
    return claims


def current_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
) -> dict[str, object]:
    """Return validated identity information from a bearer token."""
    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication is required",
            headers={"WWW-Authenticate": "Bearer"},
        )
    try:
        claims = decode_access_token(credentials.credentials)
    except PyJWTError as error:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired access token",
            headers={"WWW-Authenticate": "Bearer"},
        ) from error

    realm_access = claims.get("realm_access", {})
    roles = realm_access.get("roles", []) if isinstance(realm_access, dict) else []
    return {
        "username": claims.get("preferred_username", claims.get("sub")),
        "email": claims.get("email"),
        "roles": roles,
    }


def university_admin(
    user: dict[str, object] = Depends(current_user),
) -> dict[str, object]:
    """Require the university administrator realm role."""
    if "university-admin" not in user["roles"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="University administrator role is required",
        )
    return user


def project_payload() -> dict[str, object]:
    """Build the stable project description payload."""
    return {
        "name": "Oracle University Analytics",
        "database": ["Oracle Database 19c", "Oracle AI Database 26ai"],
        "capabilities": [
            "university information model",
            "SQL and PL/SQL analytics",
            "Random Forest",
            "SARIMAX",
        ],
    }


def publish_task(task: TaskRequest) -> str:
    """Publish a durable analytics task to RabbitMQ."""
    message_id = str(uuid4())
    parameters = pika.URLParameters(rabbitmq_url)
    parameters.socket_timeout = 3
    parameters.blocked_connection_timeout = 3
    parameters.connection_attempts = 1

    connection = pika.BlockingConnection(parameters)
    try:
        channel = connection.channel()
        channel.queue_declare(queue=rabbitmq_queue, durable=True)
        channel.basic_publish(
            exchange="",
            routing_key=rabbitmq_queue,
            body=json.dumps(
                {
                    "message_id": message_id,
                    "task_type": task.task_type,
                    "payload": task.payload,
                }
            ).encode(),
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=pika.DeliveryMode.Persistent,
                message_id=message_id,
            ),
        )
    finally:
        if connection.is_open:
            connection.close()

    return message_id


def store_report(report: ReportRequest) -> str:
    """Store a report in the configured MinIO bucket."""
    if not minio_client.bucket_exists(minio_bucket):
        minio_client.make_bucket(minio_bucket)

    object_name = f"{uuid4()}-{report.filename}"
    report_bytes = report.content.encode()
    minio_client.put_object(
        minio_bucket,
        object_name,
        BytesIO(report_bytes),
        length=len(report_bytes),
        content_type=report.content_type,
    )
    return object_name


def oracle_summary() -> dict[str, object]:
    """Read a minimal, non-sensitive status summary from FREEPDB1."""
    with oracledb.connect(
        user=required_env("ORACLE_USER"),
        password=required_env("ORACLE_PASSWORD"),
        dsn=required_env("ORACLE_DSN"),
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                select sys_context('USERENV', 'DB_NAME'),
                       sys_context('USERENV', 'CON_NAME'),
                       (select count(*) from sh.sales)
                from dual
                """
            )
            database, container, sales_rows = cursor.fetchone()
    return {
        "status": "up",
        "database": database,
        "container": container,
        "sh_sales_rows": sales_rows,
    }


@app.get("/health", tags=["Operations"])
def health() -> dict[str, str]:
    """Return a lightweight container health response."""
    try:
        redis_client.ping()
        redis_status = "up"
    except RedisError:
        redis_status = "unavailable"
    return {
        "status": "ok",
        "service": "oracle-university-api",
        "redis": redis_status,
    }


@app.get("/api/v1/project", tags=["Project"])
def project() -> dict[str, object]:
    """Describe the current project scope."""
    cache_key = "oracle-university:project:v1"
    try:
        cached = redis_client.get(cache_key)
        if cached:
            return {**json.loads(cached), "cache": "hit"}
        payload = project_payload()
        redis_client.setex(cache_key, 300, json.dumps(payload))
        return {**payload, "cache": "miss"}
    except RedisError:
        return {**project_payload(), "cache": "unavailable"}


@app.get("/api/v1/platform", tags=["Platform"])
def platform() -> dict[str, list[str]]:
    """Return the planned enterprise platform layers."""
    return {
        "delivery": ["GitLab/Jenkins", "SonarQube", "Harbor/Nexus", "Kubernetes"],
        "security": ["Keycloak", "Vault", "DevSecOps"],
        "data_services": ["Redis", "RabbitMQ", "MinIO"],
        "observability": ["Prometheus", "Grafana", "Kibana", "Dynatrace"],
        "operations": ["IaC", "DR", "7/24 operations"],
    }


@app.get("/api/v1/oracle/health", tags=["Operations"])
def oracle_health() -> dict[str, object]:
    """Verify the API's read-only connection to Oracle FREEPDB1."""
    try:
        return oracle_summary()
    except (oracledb.Error, RuntimeError) as error:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Oracle Database is temporarily unavailable",
        ) from error


@app.get("/api/v1/me", tags=["Identity"])
def me(user: dict[str, object] = Depends(current_user)) -> dict[str, object]:
    """Return the authenticated university identity."""
    return user


@app.get("/api/v1/admin/status", tags=["Identity"])
def admin_status(
    user: dict[str, object] = Depends(university_admin),
) -> dict[str, object]:
    """Return an administrator-only platform response."""
    return {
        "status": "authorized",
        "username": user["username"],
        "role": "university-admin",
    }


@app.post(
    "/api/v1/tasks",
    tags=["Tasks"],
    status_code=status.HTTP_202_ACCEPTED,
)
def create_task(task: TaskRequest) -> dict[str, str]:
    """Place an analytics task on the RabbitMQ work queue."""
    try:
        message_id = publish_task(task)
    except AMQPError as error:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Task queue is temporarily unavailable",
        ) from error

    try:
        redis_client.hset(
            f"task:{message_id}",
            mapping={
                "status": "queued",
                "message_id": message_id,
                "task_type": task.task_type,
            },
        )
        redis_client.expire(f"task:{message_id}", 86400)
    except RedisError:
        pass

    return {
        "status": "queued",
        "message_id": message_id,
        "task_type": task.task_type,
    }


@app.post(
    "/api/v1/reports",
    tags=["Reports"],
    status_code=status.HTTP_201_CREATED,
)
def create_report(report: ReportRequest) -> dict[str, str]:
    """Store an analytics report in MinIO."""
    try:
        object_name = store_report(report)
    except S3Error as error:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Report storage is temporarily unavailable",
        ) from error

    return {
        "status": "stored",
        "bucket": minio_bucket,
        "object_name": object_name,
    }


@app.get("/api/v1/tasks/{message_id}", tags=["Tasks"])
def get_task_status(message_id: str) -> dict[str, object]:
    """Return the current state and result of an analytics task."""
    bucket = "analytics-reports"
    object_name = f"sales-forecast/{message_id}.json"

    try:
        response = minio_client.get_object(bucket, object_name)
        try:
            report = json.loads(response.read().decode("utf-8"))
        finally:
            response.close()
            response.release_conn()

        return {
            "status": "completed",
            "message_id": message_id,
            "task_type": report.get("task_type"),
            "report": report,
        }
    except S3Error as error:
        if error.code not in {"NoSuchKey", "NoSuchBucket"}:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Task status is temporarily unavailable",
            ) from error

    try:
        task = redis_client.hgetall(f"task:{message_id}")
    except RedisError as error:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Task status is temporarily unavailable",
        ) from error

    if task:
        return {
            "status": task.get("status", "queued"),
            "message_id": message_id,
            "task_type": task.get("task_type"),
            "error": task.get("error"),
        }

    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="Task not found",
    )


@app.get("/api/v1/reports", tags=["Reports"])
def list_reports() -> dict[str, object]:
    """List completed analytics reports stored in MinIO."""
    reports = []

    try:
        objects = minio_client.list_objects(
            "analytics-reports",
            prefix="sales-forecast/",
            recursive=True,
        )

        for item in objects:
            response = minio_client.get_object(
                "analytics-reports",
                item.object_name,
            )
            try:
                reports.append(
                    json.loads(response.read().decode("utf-8"))
                )
            finally:
                response.close()
                response.release_conn()

    except S3Error as error:
        if error.code == "NoSuchBucket":
            return {"count": 0, "reports": []}
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Report storage is temporarily unavailable",
        ) from error

    reports.sort(
        key=lambda report: str(report.get("created_at", "")),
        reverse=True,
    )

    return {
        "count": len(reports),
        "reports": reports,
    }


@app.get("/api/v1/reports/{message_id}", tags=["Reports"])
def get_report(message_id: str) -> dict[str, object]:
    """Return a completed analytics report from MinIO."""
    bucket = "analytics-reports"
    object_name = f"sales-forecast/{message_id}.json"

    try:
        response = minio_client.get_object(bucket, object_name)
        try:
            return json.loads(response.read().decode("utf-8"))
        finally:
            response.close()
            response.release_conn()
    except S3Error as error:
        if error.code in {"NoSuchKey", "NoSuchBucket"}:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Report not found",
            ) from error
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Report storage is temporarily unavailable",
        ) from error


@app.get("/api/v1/oracle/sales-summary",tags=["operations"])
def sales_summary () -> dict[str, object]:
    with oracledb.connect(
        user=required_env("ORACLE_USER"),
        password=required_env("ORACLE_PASSWORD"),
        dsn=required_env("ORACLE_DSN"),
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*),
                       SUM(quantity_sold),
                       ROUND(SUM(amount_sold), 2),
                       ROUND(AVG(amount_sold), 2),
                       MIN(time_id),
                       MAX(time_id)
                FROM sh.sales
            """)
            row = cursor.fetchone()

    return {
        "sales_rows": row[0],
        "quantity_sold": row[1],
        "total_amount": row[2],
        "average_amount": row[3],
        "first_sale": row[4],
        "last_sale": row[5],
    }
@app.get("/api/v1/oracle/monthly-sales", tags=["Operations"])
def monthly_sales() -> list[dict[str, object]]:
    with oracledb.connect(
        user=required_env("ORACLE_USER"),
        password=required_env("ORACLE_PASSWORD"),
        dsn=required_env("ORACLE_DSN"),
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT TO_CHAR(time_id, 'YYYY-MM'),
                       SUM(quantity_sold),
                       ROUND(SUM(amount_sold), 2)
                FROM sh.sales
                GROUP BY TO_CHAR(time_id, 'YYYY-MM')
                ORDER BY 1
            """)
            rows = cursor.fetchall()

    return [
        {"month": row[0], "quantity": row[1], "amount": row[2]}
        for row in rows
    ]
@app.get("/api/v1/oracle/category-sales", tags=["Operations"])
def category_sales() -> list[dict[str, object]]:
    with oracledb.connect(
        user=required_env("ORACLE_USER"),
        password=required_env("ORACLE_PASSWORD"),
        dsn=required_env("ORACLE_DSN"),
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT p.prod_category,
                       SUM(s.quantity_sold),
                       ROUND(SUM(s.amount_sold), 2)
                FROM sh.sales s
                JOIN sh.products p ON p.prod_id = s.prod_id
                GROUP BY p.prod_category
                ORDER BY 3 DESC
            """)
            rows = cursor.fetchall()

    return [
        {"category": row[0], "quantity": row[1], "amount": row[2]}
        for row in rows
    ]

@app.get("/api/v1/dashboard", tags=["Dashboard"])
def dashboard() -> dict[str, object]:
    """Return the consolidated platform status."""
    services = {}
    overall_status = "operational"

    try:
        redis_client.ping()
        services["redis"] = {"status": "up"}
    except Exception as error:
        services["redis"] = {"status": "down", "error": str(error)}
        overall_status = "degraded"

    try:
        rabbit_url = required_env("RABBITMQ_URL")
        rabbit_connection = pika.BlockingConnection(
            pika.URLParameters(rabbit_url)
        )
        rabbit_connection.close()
        services["rabbitmq"] = {"status": "up"}
    except Exception as error:
        services["rabbitmq"] = {"status": "down", "error": str(error)}
        overall_status = "degraded"

    try:
        minio_ready = minio_client.bucket_exists(minio_bucket)
        services["minio"] = {
            "status": "up",
            "bucket": minio_bucket,
            "bucket_exists": minio_ready,
        }
    except Exception as error:
        services["minio"] = {"status": "down", "error": str(error)}
        overall_status = "degraded"

    try:
        oracle = oracle_summary()
        services["oracle"] = {"status": "up"}
    except Exception as error:
        oracle = {"status": "down", "error": str(error)}
        services["oracle"] = {"status": "down"}
        overall_status = "degraded"

    task_counts = {
        "queued": 0,
        "processing": 0,
        "completed": 0,
        "failed": 0,
    }

    try:
        for key in redis_client.scan_iter("task:*"):
            task = redis_client.hgetall(key)
            task_status = task.get("status")
            if task_status in task_counts:
                task_counts[task_status] += 1
    except Exception as error:
        task_counts["error"] = str(error)
        overall_status = "degraded"

    try:
        report_data = list_reports()
        reports = report_data.get("reports", [])
        latest_report = reports[0] if reports else None
    except Exception as error:
        reports = []
        latest_report = None
        overall_status = "degraded"

    latest_forecast = None
    worker_status = "waiting"

    if latest_report:
        latest_forecast = latest_report.get("result", {}).get("forecast")
        worker_status = "operational"

    services["analytics_worker"] = {
        "status": worker_status,
        "last_completed_report": (
            latest_report.get("message_id") if latest_report else None
        ),
    }

    return {
        "platform": "Oracle University Analytics Platform",
        "status": overall_status,
        "services": services,
        "oracle": oracle,
        "tasks": {
            "total": sum(
                value for value in task_counts.values()
                if isinstance(value, int)
            ),
            **task_counts,
        },
        "analytics": {
            "report_count": len(reports),
            "latest_method": (
                latest_report.get("result", {}).get("method")
                if latest_report else None
            ),
            "latest_forecast": latest_forecast,
        },
    }

DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="tr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Oracle University Analytics Platform</title>

<style>
*{box-sizing:border-box}
:root{
  --bg:#f5f7fa;
  --panel:#fff;
  --text:#17202a;
  --muted:#667085;
  --border:#e4e7ec;
  --dark:#111827;
  --success:#067647;
  --success-bg:#ecfdf3;
  --danger:#b42318;
  --danger-bg:#fef3f2;
  --warning:#b54708;
  --warning-bg:#fffaeb;
}

body{
  margin:0;
  background:var(--bg);
  color:var(--text);
  font-family:Inter,Arial,sans-serif;
}

.layout{
  display:grid;
  grid-template-columns:245px 1fr;
  min-height:100vh;
}

aside{
  background:#101828;
  color:#fff;
  padding:28px 20px;
}

.brand{
  font-size:19px;
  font-weight:800;
  line-height:1.35;
}

.brand small{
  display:block;
  margin-top:5px;
  color:#98a2b3;
  font-size:12px;
  font-weight:400;
}

.nav{
  margin-top:34px;
}

.nav-item{
  padding:12px 14px;
  margin:5px 0;
  border-radius:7px;
  color:#d0d5dd;
  font-size:14px;
}

.nav-item.active{
  background:#1d2939;
  color:#fff;
  font-weight:700;
}

.content{
  min-width:0;
}

header{
  background:#fff;
  border-bottom:1px solid var(--border);
  padding:20px 30px;
  display:flex;
  justify-content:space-between;
  align-items:center;
}

header h1{
  margin:0;
  font-size:23px;
}

.subtitle{
  color:var(--muted);
  margin-top:5px;
  font-size:13px;
}

.header-right{
  text-align:right;
}

#last-refresh{
  font-size:12px;
  color:var(--muted);
  margin-top:7px;
}

.auth-header{
  display:flex;
  align-items:center;
  justify-content:flex-end;
  gap:12px;
  margin-bottom:10px;
}

.auth-identity{
  display:flex;
  flex-direction:column;
  text-align:right;
}

.auth-identity strong{font-size:13px}

.auth-identity span{
  color:var(--muted);
  font-size:11px;
  margin-top:2px;
}

#auth-button{
  padding:8px 12px;
  font-size:12px;
}

body.auth-required .layout{
  display:none;
}

.auth-login-screen{
  display:none;
  min-height:100vh;
  align-items:center;
  justify-content:center;
  background:#f5f7fa;
  padding:24px;
}

body.auth-required .auth-login-screen{
  display:flex;
}

.auth-login-card{
  width:100%;
  max-width:430px;
  background:#fff;
  border:1px solid var(--border);
  border-radius:12px;
  padding:32px;
  box-shadow:0 10px 30px rgba(16,24,40,.08);
  text-align:center;
}

.auth-login-card h1{
  margin:0 0 12px;
  font-size:24px;
}

.auth-login-card p{
  color:var(--muted);
  margin-bottom:24px;
  line-height:1.5;
}

#auth-login-error{
  margin-top:16px;
  color:var(--danger);
  font-size:12px;
}


main{
  padding:28px 30px 40px;
  max-width:1500px;
}

.grid{
  display:grid;
  grid-template-columns:repeat(4,minmax(180px,1fr));
  gap:16px;
}

.card{
  background:var(--panel);
  border:1px solid var(--border);
  border-radius:10px;
  padding:19px;
  box-shadow:0 1px 2px rgba(16,24,40,.04);
}

.label{
  color:var(--muted);
  font-size:11px;
  text-transform:uppercase;
  letter-spacing:.7px;
}

.value{
  margin-top:10px;
  font-size:27px;
  font-weight:800;
}

section{
  margin-top:25px;
}

.section-head{
  display:flex;
  justify-content:space-between;
  align-items:center;
  margin-bottom:12px;
}

h2{
  margin:0;
  font-size:17px;
}

.panel{
  background:#fff;
  border:1px solid var(--border);
  border-radius:10px;
  padding:20px;
}

.status-badge{
  display:inline-flex;
  align-items:center;
  gap:7px;
  border-radius:20px;
  padding:6px 10px;
  font-size:12px;
  font-weight:700;
}

.status-up{
  color:var(--success);
  background:var(--success-bg);
}

.status-down{
  color:var(--danger);
  background:var(--danger-bg);
}

.status-waiting{
  color:var(--warning);
  background:var(--warning-bg);
}

.dot{
  width:7px;
  height:7px;
  border-radius:50%;
  background:currentColor;
}

.services{
  display:grid;
  grid-template-columns:repeat(5,1fr);
  gap:12px;
}

.service{
  border:1px solid var(--border);
  border-radius:8px;
  padding:15px;
}

.service-name{
  font-size:12px;
  color:var(--muted);
  margin-bottom:10px;
}

.chart-summary{
  display:grid;
  grid-template-columns:repeat(3,1fr);
  gap:12px;
  margin-bottom:16px;
}

.chart-summary-card{
  background:#f9fafb;
  border:1px solid var(--border);
  border-radius:8px;
  padding:14px;
}

.chart-summary-card span{
  display:block;
  color:var(--muted);
  font-size:11px;
  text-transform:uppercase;
}

.chart-summary-card strong{
  display:block;
  font-size:20px;
  margin-top:7px;
}

.chart-wrap{
  border-top:1px solid var(--border);
  padding-top:18px;
}

.chart-legend{
  display:flex;
  gap:22px;
  font-size:12px;
  color:var(--muted);
  margin-bottom:8px;
}

.legend-line{
  display:inline-block;
  width:26px;
  border-top:3px solid #111827;
  margin-right:7px;
  vertical-align:middle;
}

.forecast-line{
  border-top-style:dashed;
  border-top-color:#667085;
}

#sales-chart,#category-chart{
  width:100%;
  height:auto;
  display:block;
}

.chart-grid{stroke:#eaecf0;stroke-width:1}
.chart-axis{stroke:#98a2b3;stroke-width:1}
.chart-actual{fill:none;stroke:#101828;stroke-width:3}
.chart-predicted{fill:none;stroke:#667085;stroke-width:3;stroke-dasharray:9 7}
.chart-point{fill:#fff;stroke:#101828;stroke-width:2}
.chart-point.predicted{stroke:#667085}
.chart-separator{stroke:#98a2b3;stroke-width:1;stroke-dasharray:4 5}
.chart-label{font-size:10px;fill:#667085}
.chart-title-label{font-size:11px;fill:#344054;font-weight:bold}

.two-col{
  display:grid;
  grid-template-columns:1.4fr 1fr;
  gap:18px;
}

button{
  border:0;
  border-radius:7px;
  background:#101828;
  color:white;
  padding:12px 18px;
  font-size:14px;
  font-weight:700;
  cursor:pointer;
}

button:hover{background:#344054}
button:disabled{opacity:.5;cursor:wait}

#task-message{
  display:inline-block;
  margin-left:12px;
  font-size:13px;
  font-weight:600;
}

.task-grid{
  display:grid;
  grid-template-columns:repeat(4,1fr);
  gap:10px;
}

.task-box{
  padding:14px;
  border:1px solid var(--border);
  border-radius:8px;
}

.task-box strong{
  display:block;
  margin-top:7px;
  font-size:23px;
}

table{
  width:100%;
  border-collapse:collapse;
  font-size:12px;
}

th,td{
  text-align:left;
  padding:11px 8px;
  border-bottom:1px solid var(--border);
}

th{
  color:var(--muted);
  font-weight:600;
}

.empty{
  padding:25px 0;
  color:var(--muted);
  text-align:center;
}

footer{
  color:var(--muted);
  font-size:11px;
  margin-top:25px;
}

@media(max-width:1050px){
  .layout{grid-template-columns:1fr}
  aside{display:none}
  .grid{grid-template-columns:repeat(2,1fr)}
  .services{grid-template-columns:repeat(2,1fr)}
  .two-col{grid-template-columns:1fr}
}

@media(max-width:600px){
  header{padding:18px}
  main{padding:18px}
  .grid,.services,.task-grid,.chart-summary{grid-template-columns:1fr}
}

/* ===== Dashboard V5 Navigation ===== */

.nav-item,
.sidebar a,
.sidebar button {
    cursor: pointer;
}

.dashboard-section {
    display: none;
    animation: sectionFade .18s ease-in-out;
}

.dashboard-section.active {
    display: block;
}

@keyframes sectionFade {
    from {
        opacity: 0;
        transform: translateY(3px);
    }

    to {
        opacity: 1;
        transform: translateY(0);
    }
}

.v5-page-header {
    margin-bottom: 22px;
}

.v5-page-header h1 {
    margin: 0 0 6px 0;
    font-size: 26px;
    font-weight: 700;
}

.v5-page-header p {
    margin: 0;
    color: #64748b;
    font-size: 14px;
}

.v5-grid {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 18px;
}

.v5-card {
    background: #ffffff;
    border: 1px solid #e5e7eb;
    border-radius: 14px;
    padding: 20px;
}

.v5-card h3 {
    margin-top: 0;
}

.model-name {
    font-size: 18px;
    font-weight: 700;
}

.model-description {
    color: #64748b;
    margin-top: 8px;
    line-height: 1.6;
}

.metric-grid-v5 {
    display: grid;
    grid-template-columns: repeat(4, minmax(0, 1fr));
    gap: 12px;
    margin-top: 16px;
}

.metric-v5 {
    background: #f8fafc;
    border: 1px solid #e5e7eb;
    border-radius: 12px;
    padding: 15px;
}

.metric-v5 .label {
    color: #64748b;
    font-size: 12px;
    margin-bottom: 7px;
}

.metric-v5 .value {
    font-weight: 700;
    font-size: 18px;
}

.infrastructure-grid-v5 {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: 14px;
}

.infrastructure-card-v5 {
    background: #fff;
    border: 1px solid #e5e7eb;
    border-radius: 14px;
    padding: 18px;
}

.infrastructure-card-v5 .service-name {
    font-weight: 700;
    margin-bottom: 10px;
}

.infrastructure-card-v5 .service-description {
    color: #64748b;
    font-size: 13px;
    margin-top: 10px;
}

.status-dot-v5 {
    width: 8px;
    height: 8px;
    border-radius: 50%;
    display: inline-block;
    margin-right: 6px;
    background: #16a34a;
}

@media(max-width: 900px) {
    .v5-grid,
    .metric-grid-v5,
    .infrastructure-grid-v5 {
        grid-template-columns: 1fr;
    }
}


/* ===== Dashboard V5 Polish ===== */

.nav-item {
    transition: all .18s ease;
    border-left: 3px solid transparent;
}

.nav-item:hover {
    background: rgba(255,255,255,.06);
}

.nav-item.active {
    background: rgba(255,255,255,.10);
    border-left-color: #ffffff;
    font-weight: 700;
}

.service-status-v5 {
    display: inline-flex;
    align-items: center;
    gap: 7px;
    border-radius: 999px;
    padding: 6px 10px;
    font-size: 12px;
    font-weight: 700;
    margin-top: 2px;
}

.service-status-v5.ok {
    background: #ecfdf3;
    color: #15803d;
}

.service-status-v5.warn {
    background: #fff7ed;
    color: #c2410c;
}

.service-status-v5.down {
    background: #fef2f2;
    color: #b91c1c;
}

.service-status-v5.unknown {
    background: #f1f5f9;
    color: #475569;
}

.overview-metrics-v5 {
    margin-top: 18px;
}

.overview-metrics-v5 .metric-v5 {
    background: #ffffff;
}


/* ===== Reports Tools ===== */

.reports-toolbar-v5 {
    display: flex;
    gap: 10px;
    align-items: center;
    flex-wrap: wrap;
    margin-bottom: 14px;
}

.reports-toolbar-v5 input,
.reports-toolbar-v5 select {
    height: 38px;
    border: 1px solid #d7dde6;
    border-radius: 8px;
    padding: 0 12px;
    background: #fff;
    color: #0f172a;
    font-size: 13px;
}

.reports-toolbar-v5 input {
    min-width: 260px;
    flex: 1;
}

.reports-toolbar-v5 button {
    height: 38px;
    border: 0;
    border-radius: 8px;
    padding: 0 14px;
    background: #0f172a;
    color: #fff;
    font-weight: 700;
    cursor: pointer;
}

.reports-count-v5 {
    color: #64748b;
    font-size: 12px;
    margin-bottom: 10px;
}


/* ===== Dashboard V5.3 Reports ===== */

.report-status-badge-v53 {
    display: inline-flex;
    align-items: center;
    border-radius: 999px;
    padding: 5px 9px;
    font-size: 11px;
    font-weight: 700;
}

.report-status-badge-v53.completed {
    background: #ecfdf3;
    color: #15803d;
}

.report-status-badge-v53.failed {
    background: #fef2f2;
    color: #b91c1c;
}

.report-status-badge-v53.other {
    background: #f1f5f9;
    color: #475569;
}

.reports-pagination-v53 {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 12px;
    flex-wrap: wrap;
    margin-top: 14px;
}

.reports-pagination-v53 .pagination-info {
    color: #64748b;
    font-size: 12px;
}

.reports-pagination-v53 .pagination-actions {
    display: flex;
    align-items: center;
    gap: 8px;
}

.reports-pagination-v53 button {
    border: 1px solid #d7dde6;
    background: #ffffff;
    color: #0f172a;
    border-radius: 8px;
    padding: 7px 12px;
    cursor: pointer;
    font-weight: 600;
}

.reports-pagination-v53 button:disabled {
    opacity: .45;
    cursor: not-allowed;
}

.reports-pagination-v53 .page-number {
    min-width: 80px;
    text-align: center;
    font-size: 12px;
    color: #475569;
}

@media(max-width: 900px) {
    .reports-toolbar-v5 input {
        min-width: 100%;
    }
}

</style>
</head>

<body class="auth-required">

<div id="auth-login-screen" class="auth-login-screen">
  <div class="auth-login-card">
    <h1>Oracle University Analytics</h1>
    <p>Dashboard'a erişmek için Keycloak hesabınızla giriş yapın.</p>
    <button type="button" onclick="loginWithKeycloak()">Keycloak ile Giriş Yap</button>
    <div id="auth-login-error"></div>
  </div>
</div>
<div class="layout">

<aside>
  <div class="brand">
    Oracle University
    <small>Analytics Platform</small>
  </div>

  <div class="nav">
    <div class="nav-item active" data-section="overview">Genel Bakış</div>
    <div class="nav-item" data-section="sales">Satış Analitiği</div>
    <div class="nav-item" data-section="models">Tahmin Modelleri</div>
    <div class="nav-item" data-section="reports">Raporlar</div>
    <div class="nav-item" data-section="infrastructure">Altyapı</div>
  </div>
</aside>

<div class="content">

<header>
  <div>
    <h1>Analytics Dashboard</h1>
    <div class="subtitle">Oracle SH verileri ve SARIMAX satış tahminleri</div>
  </div>

  <div class="header-right">
    <div class="auth-header">
      <div class="auth-identity">
        <strong id="auth-user-name">Oturum kapalı</strong>
        <span id="auth-user-role">—</span>
      </div>
      <button id="auth-button" type="button">Giriş Yap</button>
    </div>

    <div id="platform-status" class="status-badge status-waiting">
      <span class="dot"></span>Kontrol ediliyor
    </div>
    <div id="last-refresh">—</div>
  </div>
</header>


<main>

<!-- ===================================================== -->
<!-- 1. GENEL BAKIŞ -->
<!-- ===================================================== -->
<div id="section-overview" class="dashboard-section active">

  <div class="v5-page-header">
    <h1>Genel Bakış</h1>
    <p>Oracle University Analytics Platform genel sistem ve iş yükü özeti.</p>
  </div>

  <div class="grid">
    <div class="card">
      <div class="label">SH.SALES Kayıtları</div>
      <div class="value" id="sales">—</div>
    </div>

    <div class="card">
      <div class="label">Toplam Görev</div>
      <div class="value" id="tasks">—</div>
    </div>

    <div class="card">
      <div class="label">Tamamlanan Rapor</div>
      <div class="value" id="reports">—</div>
    </div>

    <div class="card">
      <div class="label">Aktif Model</div>
      <div class="value" id="model" style="font-size:20px">—</div>
    </div>
  </div>

  <section class="overview-metrics-v5">
    <div class="section-head">
      <h2>Model Performansı</h2>
    </div>

    <div class="metric-grid-v5">

      <div class="metric-v5">
        <div class="label">MAE</div>
        <div class="value" id="overview-mae-v5">—</div>
      </div>

      <div class="metric-v5">
        <div class="label">RMSE</div>
        <div class="value" id="overview-rmse-v5">—</div>
      </div>

      <div class="metric-v5">
        <div class="label">MAPE</div>
        <div class="value" id="overview-mape-v5">—</div>
      </div>

      <div class="metric-v5">
        <div class="label">Doğrulama</div>
        <div class="value" id="overview-holdout-v5">—</div>
      </div>

    </div>
  </section>


  <section>
    <div class="section-head">
      <h2>Platform Servisleri</h2>
    </div>

    <div id="services" class="services"></div>
  </section>

  <section>
    <div class="section-head">
      <h2>Görev Dağılımı</h2>
    </div>

    <div class="panel">
      <div class="task-grid">

        <div class="task-box">
          <span class="label">Kuyrukta</span>
          <strong id="queued">0</strong>
        </div>

        <div class="task-box">
          <span class="label">İşleniyor</span>
          <strong id="processing">0</strong>
        </div>

        <div class="task-box">
          <span class="label">Tamamlandı</span>
          <strong id="completed">0</strong>
        </div>

        <div class="task-box">
          <span class="label">Başarısız</span>
          <strong id="failed">0</strong>
        </div>

      </div>
    </div>
  </section>

</div>


<!-- ===================================================== -->
<!-- 2. SATIŞ ANALİTİĞİ -->
<!-- ===================================================== -->
<div id="section-sales" class="dashboard-section">

  <div class="v5-page-header">
    <h1>Satış Analitiği</h1>
    <p>Oracle SH şemasındaki gerçek satış verilerinin dönemsel ve kategorik analizi.</p>
  </div>

  <section>
    <div class="section-head">
      <h2>Satış Gerçekleşmeleri ve Tahmin</h2>
    </div>

    <div class="panel">

      <div class="chart-summary">

        <div class="chart-summary-card">
          <span>Son Gerçekleşen Satış</span>
          <strong id="last-actual">—</strong>
        </div>

        <div class="chart-summary-card">
          <span>İlk Tahmin</span>
          <strong id="first-forecast">—</strong>
        </div>

        <div class="chart-summary-card">
          <span>Beklenen Değişim</span>
          <strong id="forecast-change">—</strong>
        </div>

      </div>

      <div class="chart-wrap">

        <div class="chart-legend">
          <span>
            <i class="legend-line"></i>
            Son 12 ay gerçek satış
          </span>

          <span>
            <i class="legend-line forecast-line"></i>
            3 aylık SARIMAX tahmini
          </span>
        </div>

        <svg id="sales-chart" viewBox="0 0 1100 360"></svg>

      </div>
    </div>
  </section>

  <section>
    <div class="section-head">
      <h2>Kategori Bazlı Satışlar</h2>
    </div>

    <div class="panel">
      <svg id="category-chart" viewBox="0 0 720 300"></svg>
    </div>
  </section>

</div>


<!-- ===================================================== -->
<!-- 3. TAHMİN MODELLERİ -->
<!-- ===================================================== -->
<div id="section-models" class="dashboard-section">

  <div class="v5-page-header">
    <h1>Tahmin Modelleri</h1>
    <p>SARIMAX satış tahmin modeli, doğrulama metrikleri ve yeni tahmin üretimi.</p>
  </div>

  <div class="v5-card">

    <div class="model-name">
      SARIMAX(1,1,1)(1,1,1,12)
    </div>

    <div class="model-description">
      Oracle SH satış verilerinin aylık zaman serisi kullanılarak
      mevsimsel satış tahmini üretilir. Model performansı son 6 aylık
      holdout doğrulama dönemi üzerinden ölçülür.
    </div>

    <div class="metric-grid-v5">

      <div class="metric-v5">
        <div class="label">MAE</div>
        <div class="value" id="model-mae-v5">—</div>
      </div>

      <div class="metric-v5">
        <div class="label">RMSE</div>
        <div class="value" id="model-rmse-v5">—</div>
      </div>

      <div class="metric-v5">
        <div class="label">MAPE</div>
        <div class="value" id="model-mape-v5">—</div>
      </div>

      <div class="metric-v5">
        <div class="label">Doğrulama Dönemi</div>
        <div class="value" id="model-holdout-v5">—</div>
      </div>

    </div>
  </div>

  <section>

    
  <section>
    <div class="section-head">
      <h2>Model Karşılaştırması</h2>
    </div>

    <div class="metric-grid-v5">

      <div class="metric-v5">
        <div class="label">SARIMAX MAPE</div>
        <div class="value" id="v6-sarimax-mape">—</div>
      </div>

      <div class="metric-v5">
        <div class="label">Seasonal Naive MAPE</div>
        <div class="value" id="v6-baseline-mape">—</div>
      </div>

      <div class="metric-v5">
        <div class="label">Seçilen Model</div>
        <div class="value" id="v6-best-model" style="font-size:16px">—</div>
      </div>

      <div class="metric-v5">
        <div class="label">Seçim Kriteri</div>
        <div class="value" style="font-size:16px">MAPE</div>
      </div>

    </div>
  </section>

<div class="section-head">
      <h2>Yeni Tahmin Oluştur</h2>
    </div>

    <div class="panel">

      <p style="color:#667085;font-size:13px;margin-top:0">
        Oracle SH satış verileri RabbitMQ üzerinden analytics-worker
        servisine gönderilir ve üç aylık SARIMAX tahmini oluşturulur.
      </p>

      <select id="forecast-months-v6"
        style="height:38px;border:1px solid #d7dde6;border-radius:8px;padding:0 10px;margin-right:8px">
        <option value="3">3 Ay</option>
        <option value="6">6 Ay</option>
        <option value="12">12 Ay</option>
      </select>

      <button id="forecast-button" onclick="createForecast()">
        Yeni Tahmin Oluştur
      </button>

      <span id="task-message"></span>

    </div>

  </section>

  <section>

    <div class="section-head">
      <h2>Model Akışı</h2>
    </div>

    <div class="v5-card">

      <div class="model-description">
        Oracle SH.SALES
        &nbsp;→&nbsp;
        FastAPI
        &nbsp;→&nbsp;
        RabbitMQ
        &nbsp;→&nbsp;
        Analytics Worker
        &nbsp;→&nbsp;
        SARIMAX
        &nbsp;→&nbsp;
        MinIO
        &nbsp;→&nbsp;
        Dashboard
      </div>

    </div>

  </section>

</div>


<!-- ===================================================== -->
<!-- 4. RAPORLAR -->
<!-- ===================================================== -->
<div id="section-reports" class="dashboard-section">

  <div class="v5-page-header">
    <h1>Raporlar</h1>
    <p>Üretilen satış tahmin raporları ve model doğrulama sonuçları.</p>
  </div>

  <section>

    <div class="section-head">
      <h2>Son Tahmin Raporları</h2>
    </div>

    <div class="panel">

      <div class="reports-toolbar-v5">
        <input
          id="report-search-v5"
          type="search"
          placeholder="Görev ID, model veya tür ara..."
        >

        <select id="report-status-filter-v5">
          <option value="all">Tüm Durumlar</option>
          <option value="completed">Tamamlandı</option>
          <option value="failed">Başarısız</option>
        </select>

        <input
          id="report-date-start-v53"
          type="date"
          title="Başlangıç tarihi"
        >

        <input
          id="report-date-end-v53"
          type="date"
          title="Bitiş tarihi"
        >

        <select id="report-page-size-v53" title="Sayfa başına rapor">
          <option value="10">10 / sayfa</option>
          <option value="20">20 / sayfa</option>
          <option value="50">50 / sayfa</option>
        </select>

        <button type="button" onclick="exportReportsCsvV5()">
          CSV Dışa Aktar
        </button>
      </div>

      <div id="reports-count-v5" class="reports-count-v5">—</div>

      <div id="reports-table">
        <div class="empty">
          Raporlar yükleniyor...
        </div>
      </div>

      <div class="reports-pagination-v53">

        <div id="reports-pagination-info-v53" class="pagination-info">
          —
        </div>

        <div class="pagination-actions">

          <button
            id="reports-prev-v53"
            type="button"
            onclick="previousReportsPageV53()"
          >
            Önceki
          </button>

          <span id="reports-page-number-v53" class="page-number">
            Sayfa 1 / 1
          </span>

          <button
            id="reports-next-v53"
            type="button"
            onclick="nextReportsPageV53()"
          >
            Sonraki
          </button>

        </div>

      </div>

    </div>

  </section>

  <section>

    <div class="section-head">
      <h2>Rapor Detayı</h2>
    </div>

    <div class="panel" id="report-detail">
      <div class="empty">
        Detayını görmek için yukarıdaki raporlardan birini seçin.
      </div>
    </div>

  </section>

</div>


<!-- ===================================================== -->
<!-- 5. ALTYAPI -->
<!-- ===================================================== -->
<div id="section-infrastructure" class="dashboard-section">

  <div class="v5-page-header">
    <h1>Altyapı</h1>
    <p>Kubernetes üzerinde çalışan platform servislerinin operasyonel durumu.</p>
  </div>

  <div class="infrastructure-grid-v5">

    <div class="infrastructure-card-v5">
      <div class="service-name">
        <span class="status-dot-v5"></span>
        Oracle Database
      </div>

      <strong id="infra-oracle-v5">Kontrol ediliyor</strong>

      <div class="service-description">
        Oracle Database FREEPDB1 ve SH satış verileri.
      </div>
    </div>


    <div class="infrastructure-card-v5">
      <div class="service-name">
        <span class="status-dot-v5"></span>
        Redis
      </div>

      <strong id="infra-redis-v5">Kontrol ediliyor</strong>

      <div class="service-description">
        Görev durumları ve platform önbelleği.
      </div>
    </div>


    <div class="infrastructure-card-v5">
      <div class="service-name">
        <span class="status-dot-v5"></span>
        RabbitMQ
      </div>

      <strong id="infra-rabbitmq-v5">Kontrol ediliyor</strong>

      <div class="service-description">
        Asenkron analitik görev kuyruğu.
      </div>
    </div>


    <div class="infrastructure-card-v5">
      <div class="service-name">
        <span class="status-dot-v5"></span>
        MinIO
      </div>

      <strong id="infra-minio-v5">Kontrol ediliyor</strong>

      <div class="service-description">
        Tahmin raporlarının JSON nesne depolaması.
      </div>
    </div>


    <div class="infrastructure-card-v5">
      <div class="service-name">
        <span class="status-dot-v5"></span>
        Analytics Worker
      </div>

      <strong id="infra-worker-v5">Kontrol ediliyor</strong>

      <div class="service-description">
        SARIMAX tahmin ve model doğrulama servisi.
      </div>
    </div>


    <div class="infrastructure-card-v5">
      <div class="service-name">
        <span class="status-dot-v5"></span>
        Kubernetes / K3s
      </div>

      <strong>ACTIVE</strong>

      <div class="service-description">
        Uygulama servislerinin container orchestration katmanı.
      </div>
    </div>

  </div>

  <section>

    <div class="section-head">
      <h2>Platform Mimarisi</h2>
    </div>

    <div class="v5-card">

      <div class="model-description">
        Nginx Gateway
        &nbsp;→&nbsp;
        FastAPI
        &nbsp;→&nbsp;
        RabbitMQ / Redis
        &nbsp;→&nbsp;
        Analytics Worker
        &nbsp;→&nbsp;
        Oracle / MinIO
      </div>

    </div>

  </section>

</div>


<footer>
  Oracle University Analytics Platform • Veriler 15 saniyede bir otomatik yenilenir.
</footer>

</main>

</div>
</div>

<script>
const fmt=v=>Number(v).toLocaleString("tr-TR",{maximumFractionDigits:0});

function badge(status){
  const s=String(status||'waiting').toLowerCase();
  const cls=s==='up'||s==='operational' ? 'status-up'
           : s==='down'||s==='failed' ? 'status-down'
           : 'status-waiting';

  return '<span class="status-badge '+cls+'"><span class="dot"></span>'+
         String(status||'waiting').toUpperCase()+'</span>';
}

async function createForecast(){
  const button=document.getElementById('forecast-button');
  const message=document.getElementById('task-message');

  button.disabled=true;
  message.textContent='Görev kuyruğa gönderiliyor...';

  try{
    const response=await authFetch('/api/v1/tasks',{
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({
        task_type:'sales-forecast',
        payload:{
          source:'oracle-sh',
          months:Number(
            document.getElementById('forecast-months-v6')?.value || 3
          )
        }
      })
    });

    const task=await response.json();
    if(!response.ok) throw new Error(task.detail||'Görev oluşturulamadı');

    message.textContent='Tahmin hazırlanıyor...';

    for(let i=0;i<30;i++){
      await new Promise(resolve=>setTimeout(resolve,2000));

      const r=await authFetch('/api/v1/tasks/'+task.message_id);
      const data=await r.json();

      if(data.status==='completed'){
        message.textContent='Tahmin başarıyla tamamlandı.';
        await loadDashboard();
        return;
      }

      if(data.status==='failed'){
        throw new Error(data.error||'Tahmin görevi başarısız oldu');
      }
    }

    message.textContent='Görev devam ediyor; panel otomatik yenilenecek.';
  }catch(error){
    message.textContent='Hata: '+error.message;
  }finally{
    button.disabled=false;
  }
}

function renderSalesChart(monthlySales,forecast){
  const svg=document.getElementById('sales-chart');
  if(!svg)return;

  const actual=(monthlySales||[]).slice(-12).map(item=>({
    month:String(item.month),
    amount:Number(item.amount),
    type:'actual'
  }));

  const predicted=(forecast||[]).map(item=>({
    month:String(item.month),
    amount:Number(item.predicted_amount),
    type:'predicted'
  }));

  const lastActual=actual.length?actual[actual.length-1].amount:null;
  const firstForecast=predicted.length?predicted[0].amount:null;
  const change=(lastActual&&firstForecast)
    ?((firstForecast-lastActual)/lastActual)*100
    :null;

  document.getElementById('last-actual').textContent=
    lastActual===null?'—':fmt(lastActual);

  document.getElementById('first-forecast').textContent=
    firstForecast===null?'—':fmt(firstForecast);

  document.getElementById('forecast-change').textContent=
    change===null?'—':(change>=0?'+':'')+change.toFixed(2)+'%';

  const points=actual.concat(predicted);

  if(points.length<2){
    svg.innerHTML='<text x="40" y="70" class="chart-title-label">Grafik için veri bekleniyor.</text>';
    return;
  }

  const W=1100,H=360,L=82,R=28,T=28,B=62;
  const plotW=W-L-R,plotH=H-T-B;
  const values=points.map(p=>p.amount);

  let min=Math.min(...values),max=Math.max(...values);
  const padding=Math.max((max-min)*0.18,max*0.04,1);
  min=Math.max(0,min-padding);
  max=max+padding;

  const x=i=>L+(i/(points.length-1))*plotW;
  const y=v=>T+(max-v)/(max-min)*plotH;

  const pathFor=(items,startIndex)=>items.map((p,i)=>
    (i?'L':'M')+x(startIndex+i).toFixed(1)+' '+y(p.amount).toFixed(1)
  ).join(' ');

  let html='';

  for(let i=0;i<=4;i++){
    const yy=T+(i/4)*plotH;
    const value=max-(i/4)*(max-min);

    html+='<line class="chart-grid" x1="'+L+'" y1="'+yy+
      '" x2="'+(W-R)+'" y2="'+yy+'"></line>';

    html+='<text class="chart-label" x="'+(L-10)+'" y="'+(yy+4)+
      '" text-anchor="end">'+fmt(value)+'</text>';
  }

  html+='<line class="chart-axis" x1="'+L+'" y1="'+(H-B)+
    '" x2="'+(W-R)+'" y2="'+(H-B)+'"></line>';

  html+='<path class="chart-actual" d="'+pathFor(actual,0)+'"></path>';

  if(predicted.length){
    const bridge=[actual[actual.length-1]].concat(predicted);

    html+='<path class="chart-predicted" d="'+
      pathFor(bridge,actual.length-1)+'"></path>';

    const separatorX=(x(actual.length-1)+x(actual.length))/2;

    html+='<line class="chart-separator" x1="'+separatorX+'" y1="'+T+
      '" x2="'+separatorX+'" y2="'+(H-B)+'"></line>';

    html+='<text class="chart-title-label" x="'+(separatorX+7)+
      '" y="'+(T+12)+'">Tahmin başlangıcı</text>';
  }

  points.forEach((p,i)=>{
    html+='<circle class="chart-point '+(p.type==='predicted'?'predicted':'')+
      '" cx="'+x(i)+'" cy="'+y(p.amount)+'" r="4"></circle>';

    html+='<text class="chart-label" x="'+x(i)+'" y="'+(H-B+22)+
      '" text-anchor="end" transform="rotate(-45 '+x(i)+' '+(H-B+22)+')">'+
      p.month+'</text>';
  });

  svg.innerHTML=html;
}

function renderCategoryChart(items){
  const svg=document.getElementById('category-chart');
  const rows=(items||[]).slice(0,6);

  if(!rows.length){
    svg.innerHTML='<text x="30" y="50" class="chart-title-label">Kategori verisi bulunamadı.</text>';
    return;
  }

  const W=720,H=300,L=150,R=30,T=20,B=25;
  const max=Math.max(...rows.map(x=>Number(x.amount)||0))||1;
  const rowH=(H-T-B)/rows.length;

  let html='';

  rows.forEach((item,i)=>{
    const amount=Number(item.amount)||0;
    const y=T+i*rowH+8;
    const width=((W-L-R)*amount/max);

    html+='<text class="chart-label" x="'+(L-10)+'" y="'+(y+14)+
      '" text-anchor="end">'+String(item.category)+'</text>';

    html+='<rect x="'+L+'" y="'+y+'" width="'+width+
      '" height="20" rx="3" fill="#344054"></rect>';

    html+='<text class="chart-label" x="'+(L+width+7)+'" y="'+(y+14)+
      '">'+fmt(amount)+'</text>';
  });

  svg.innerHTML=html;
}

function renderReports(data){
  const root=document.getElementById('reports-table');
  const reports=(data&&data.reports)||[];

  if(!reports.length){
    root.innerHTML='<div class="empty">Henüz tamamlanmış tahmin raporu bulunmuyor.</div>';
    return;
  }

  const rows=reports.slice(0,8).map(report=>{
    const result=report.result||{};
    const created=report.created_at
      ?new Date(report.created_at).toLocaleString('tr-TR')
      :'—';

    return '<tr style="cursor:pointer" data-id="'+
      String(report.message_id||'')+'" onclick="showReport(this.dataset.id)">'+
      '<td>'+String(report.message_id||'—').slice(0,12)+'</td>'+
      '<td>'+String(report.task_type||'sales-forecast')+'</td>'+
      '<td>'+String(result.method||'—')+'</td>'+
      '<td>'+created+'</td>'+
      '<td>'+badge('completed')+'</td>'+
      '</tr>';
  }).join('');

  root.innerHTML=
    '<table>'+
    '<thead><tr>'+
    '<th>Görev ID</th>'+
    '<th>Tür</th>'+
    '<th>Model</th>'+
    '<th>Oluşturulma</th>'+
    '<th>Durum</th>'+
    '</tr></thead>'+
    '<tbody>'+rows+'</tbody>'+
    '</table>';
}


async function showReport(messageId){
  const root=document.getElementById('report-detail');
  root.innerHTML='<div class="empty">Rapor yükleniyor...</div>';

  try{
    const response=await authFetch('/api/v1/reports/'+messageId);
    if(!response.ok)throw new Error('Rapor alınamadı');

    const report=await response.json();
    const result=report.result||{};
    const forecast=result.forecast||[];

    const forecastRows=forecast.map(item=>
      '<tr><td>'+String(item.month||'—')+'</td>'+
      '<td>'+fmt(item.predicted_amount||0)+'</td></tr>'
    ).join('');

    const v=result.validation||{};

    root.innerHTML=
      '<div class="grid" style="grid-template-columns:repeat(4,1fr);margin-bottom:18px">'+
        '<div class="card"><div class="label">Model</div><div class="value" style="font-size:16px">'+String(result.method||'—')+'</div></div>'+
        '<div class="card"><div class="label">Kaynak Dönem</div><div class="value" style="font-size:16px">'+String(result.source_start||'—')+' → '+String(result.source_end||'—')+'</div></div>'+
        '<div class="card"><div class="label">Kaynak Ay</div><div class="value">'+String(result.source_months||0)+'</div></div>'+
        '<div class="card"><div class="label">Görev ID</div><div class="value" style="font-size:13px;word-break:break-all">'+String(report.message_id||'—')+'</div></div>'+
      '</div>'+
      '<div class="grid" style="grid-template-columns:repeat(4,1fr);margin-bottom:18px">'+
        '<div class="card"><div class="label">MAE</div><div class="value">'+(v.mae!=null?fmt(v.mae):'—')+'</div></div>'+
        '<div class="card"><div class="label">RMSE</div><div class="value">'+(v.rmse!=null?fmt(v.rmse):'—')+'</div></div>'+
        '<div class="card"><div class="label">MAPE</div><div class="value">'+(v.mape!=null?Number(v.mape).toFixed(2)+'%':'—')+'</div></div>'+
        '<div class="card"><div class="label">Doğrulama</div><div class="value" style="font-size:16px">'+(v.holdout_months?String(v.holdout_months)+' ay':'—')+'</div></div>'+
      '</div>'+
      '<table>'+
        '<thead><tr><th>Tahmin Ayı</th><th>Tahmini Satış</th></tr></thead>'+
        '<tbody>'+forecastRows+'</tbody>'+
      '</table>';
  }catch(error){
    root.innerHTML='<div class="empty">Hata: '+error.message+'</div>';
  }
}


const KEYCLOAK_BASE = "http://192.168.56.102:30081";
const KEYCLOAK_REALM = "university";
const KEYCLOAK_DASHBOARD_CLIENT = "university-dashboard";
const DASHBOARD_REDIRECT_URI = window.location.origin + "/dashboard";

function base64UrlEncode(buffer){
  return btoa(String.fromCharCode(...new Uint8Array(buffer)))
    .replace(/\+/g,"-")
    .replace(/\//g,"_")
    .replace(/=+$/,"");
}

function randomPkceString(length=64){
  const chars =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~";

  const values = new Uint8Array(length);
  crypto.getRandomValues(values);

  return Array.from(
    values,
    value => chars[value % chars.length]
  ).join("");
}

async function loginWithKeycloak(){
  const verifier = randomPkceString(72);

  const digest = await crypto.subtle.digest(
    "SHA-256",
    new TextEncoder().encode(verifier)
  );

  const challenge = base64UrlEncode(digest);
  const state = randomPkceString(32);

  sessionStorage.setItem("university-pkce-verifier", verifier);
  sessionStorage.setItem("university-oauth-state", state);

  const params = new URLSearchParams({
    client_id: KEYCLOAK_DASHBOARD_CLIENT,
    redirect_uri: DASHBOARD_REDIRECT_URI,
    response_type: "code",
    scope: "openid profile email",
    state: state,
    code_challenge: challenge,
    code_challenge_method: "S256"
  });

  window.location.href =
    KEYCLOAK_BASE +
    "/realms/" +
    KEYCLOAK_REALM +
    "/protocol/openid-connect/auth?" +
    params.toString();
}

function clearDashboardAuth(){
  sessionStorage.removeItem("university-access-token");
  sessionStorage.removeItem("university-refresh-token");
  sessionStorage.removeItem("university-token-expiry");
}

async function exchangeAuthorizationCode(code){
  const verifier =
    sessionStorage.getItem("university-pkce-verifier");

  if(!verifier){
    throw new Error("PKCE doğrulama bilgisi bulunamadı.");
  }

  const body = new URLSearchParams({
    grant_type: "authorization_code",
    client_id: KEYCLOAK_DASHBOARD_CLIENT,
    code: code,
    redirect_uri: DASHBOARD_REDIRECT_URI,
    code_verifier: verifier
  });

  const response = await window.fetch(
    KEYCLOAK_BASE +
      "/realms/" +
      KEYCLOAK_REALM +
      "/protocol/openid-connect/token",
    {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded"
      },
      body: body
    }
  );

  if(!response.ok){
    throw new Error("Keycloak token oluşturamadı.");
  }

  const token = await response.json();

  sessionStorage.setItem(
    "university-access-token",
    token.access_token
  );

  if(token.refresh_token){
    sessionStorage.setItem(
      "university-refresh-token",
      token.refresh_token
    );
  }

  sessionStorage.setItem(
    "university-token-expiry",
    String(Date.now() + Number(token.expires_in || 300) * 1000)
  );

  sessionStorage.removeItem("university-pkce-verifier");
  sessionStorage.removeItem("university-oauth-state");
}

async function refreshDashboardToken(){
  const refreshToken =
    sessionStorage.getItem("university-refresh-token");

  if(!refreshToken){
    return null;
  }

  const body = new URLSearchParams({
    grant_type: "refresh_token",
    client_id: KEYCLOAK_DASHBOARD_CLIENT,
    refresh_token: refreshToken
  });

  const response = await window.fetch(
    KEYCLOAK_BASE +
      "/realms/" +
      KEYCLOAK_REALM +
      "/protocol/openid-connect/token",
    {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded"
      },
      body: body
    }
  );

  if(!response.ok){
    clearDashboardAuth();
    return null;
  }

  const token = await response.json();

  sessionStorage.setItem(
    "university-access-token",
    token.access_token
  );

  if(token.refresh_token){
    sessionStorage.setItem(
      "university-refresh-token",
      token.refresh_token
    );
  }

  sessionStorage.setItem(
    "university-token-expiry",
    String(Date.now() + Number(token.expires_in || 300) * 1000)
  );

  return token.access_token;
}

async function ensureDashboardToken(){
  const token =
    sessionStorage.getItem("university-access-token");

  if(!token){
    return null;
  }

  const expiry =
    Number(sessionStorage.getItem("university-token-expiry") || 0);

  if(expiry && Date.now() >= expiry - 30000){
    return await refreshDashboardToken();
  }

  return token;
}

async function authFetch(url, options={}){
  const token = await ensureDashboardToken();

  if(!token){
    throw new Error("AUTHENTICATION_REQUIRED");
  }

  const headers = new Headers(options.headers || {});

  headers.set("Authorization", "Bearer " + token);

  const response =
    await window.fetch(
      url,
      {
        ...options,
        headers: headers
      }
    );

  if(response.status === 401){
    clearDashboardAuth();
  }

  return response;
}

function roleLabel(roles){
  if(roles.includes("university-admin")){
    return "Yönetici";
  }

  if(roles.includes("analyst")){
    return "Analist";
  }

  if(roles.includes("viewer")){
    return "Görüntüleyici";
  }

  return "Yetkisiz";
}

function applyDashboardRolePermissions(roles){
  const canForecast =
    roles.includes("analyst") ||
    roles.includes("university-admin");

  document.querySelectorAll("button").forEach(button => {
    const value =
      (button.textContent || "").toLocaleLowerCase("tr-TR");

    if(
      value.includes("tahmin") ||
      value.includes("forecast")
    ){
      button.disabled = !canForecast;

      if(!canForecast){
        button.title =
          "Bu işlem analyst veya administrator rolü gerektirir.";
      }
    }
  });
}

function renderAuthenticatedUser(user){
  const roles =
    Array.isArray(user.roles) ? user.roles : [];

  document.getElementById("auth-user-name").textContent =
    user.username || "Kullanıcı";

  document.getElementById("auth-user-role").textContent =
    roleLabel(roles);

  const button =
    document.getElementById("auth-button");

  button.textContent = "Çıkış Yap";
  button.onclick = logoutFromKeycloak;

  applyDashboardRolePermissions(roles);

  document.body.classList.remove("auth-required");
}

function renderLoggedOut(message=""){
  document.body.classList.add("auth-required");

  const error =
    document.getElementById("auth-login-error");

  if(error){
    error.textContent = message;
  }
}

function logoutFromKeycloak(){
  clearDashboardAuth();

  const params =
    new URLSearchParams({
      client_id: KEYCLOAK_DASHBOARD_CLIENT,
      post_logout_redirect_uri: DASHBOARD_REDIRECT_URI
    });

  window.location.href =
    KEYCLOAK_BASE +
    "/realms/" +
    KEYCLOAK_REALM +
    "/protocol/openid-connect/logout?" +
    params.toString();
}

async function initializeDashboardAuth(){
  try{
    const params =
      new URLSearchParams(window.location.search);

    const code = params.get("code");
    const state = params.get("state");

    if(code){
      const expectedState =
        sessionStorage.getItem("university-oauth-state");

      if(!state || state !== expectedState){
        throw new Error("OAuth state doğrulaması başarısız.");
      }

      await exchangeAuthorizationCode(code);

      history.replaceState(
        {},
        document.title,
        "/dashboard"
      );
    }

    const token =
      await ensureDashboardToken();

    if(!token){
      renderLoggedOut();
      return;
    }

    const response =
      await authFetch("/api/v1/me");

    if(!response.ok){
      throw new Error("Kullanıcı bilgisi alınamadı.");
    }

    const user = await response.json();

    renderAuthenticatedUser(user);

    await loadDashboard();

    setInterval(loadDashboard, 15000);

  }catch(error){
    console.error("Dashboard authentication:", error);

    clearDashboardAuth();

    renderLoggedOut(
      error.message || "Oturum açılamadı."
    );
  }
}


async function loadDashboard(){
  try{
    const [dashboardResponse,salesResponse,categoryResponse,reportsResponse]=
      await Promise.all([
        authFetch('/api/v1/dashboard'),
        authFetch('/api/v1/oracle/monthly-sales'),
        authFetch('/api/v1/oracle/category-sales'),
        authFetch('/api/v1/reports')
      ]);

    if(!dashboardResponse.ok)throw new Error('Dashboard API yanıt vermedi');

    const data=await dashboardResponse.json();
    const monthlySales=salesResponse.ok?await salesResponse.json():[];
    const categories=categoryResponse.ok?await categoryResponse.json():[];
    const reports=reportsResponse.ok?await reportsResponse.json():{reports:[]};

    document.getElementById('sales').textContent=
      Number(data.oracle.sh_sales_rows||0).toLocaleString('tr-TR');

    document.getElementById('tasks').textContent=data.tasks.total||0;
    document.getElementById('reports').textContent=data.analytics.report_count||0;
    document.getElementById('model').textContent=
      data.analytics.latest_method||'SARIMAX';

    ['queued','processing','completed','failed'].forEach(key=>{
      document.getElementById(key).textContent=data.tasks[key]||0;
    });

    const platform=document.getElementById('platform-status');
    const ps=String(data.status||'unknown').toLowerCase();

    platform.className='status-badge '+
      (ps==='operational'?'status-up':'status-waiting');

    platform.innerHTML='<span class="dot"></span>'+
      String(data.status||'unknown').toUpperCase();

    document.getElementById('services').innerHTML=
      Object.entries(data.services).map(([name,item])=>
        '<div class="service">'+
        '<div class="service-name">'+name.replace('_',' ').toUpperCase()+'</div>'+
        badge(item.status)+
        '</div>'
      ).join('');

    renderSalesChart(monthlySales,data.analytics.latest_forecast||[]);
    renderCategoryChart(categories);
    renderReports(reports);

    document.getElementById('last-refresh').textContent=
      'Son güncelleme: '+new Date().toLocaleTimeString('tr-TR');

  }catch(error){
    const platform=document.getElementById('platform-status');
    platform.className='status-badge status-down';
    platform.innerHTML='<span class="dot"></span>BAĞLANTI HATASI';
    document.getElementById('last-refresh').textContent=error.message;
  }
}

initializeDashboardAuth();


// ============================================================
// Dashboard V5 Navigation
// ============================================================

function openDashboardSection(sectionName) {
    const sections = document.querySelectorAll(".dashboard-section");

    sections.forEach(section => {
        section.classList.remove("active");
    });

    const target = document.getElementById("section-" + sectionName);

    if (target) {
        target.classList.add("active");
    }

    document.querySelectorAll("[data-section]").forEach(item => {
        item.classList.remove("active");

        if (item.dataset.section === sectionName) {
            item.classList.add("active");
        }
    });

    try {
        localStorage.setItem("dashboard-active-section", sectionName);
    } catch (_) {}
}


function setupDashboardNavigation() {

    document.querySelectorAll("[data-section]").forEach(item => {

        item.addEventListener("click", function(event) {

            event.preventDefault();

            const section = this.dataset.section;

            if (section) {
                openDashboardSection(section);
            }

        });

    });

    let initialSection = "overview";

    try {
        const stored = localStorage.getItem("dashboard-active-section");

        if (stored) {
            initialSection = stored;
        }
    } catch (_) {}

    openDashboardSection(initialSection);
}


function getLatestValidation(reportList) {

    if (!Array.isArray(reportList)) {
        return null;
    }

    for (const report of reportList) {

        const validation =
            report &&
            report.result &&
            report.result.validation;

        if (
            validation &&
            validation.mae !== undefined &&
            validation.rmse !== undefined &&
            validation.mape !== undefined
        ) {
            return {
                report: report,
                validation: validation
            };
        }
    }

    return null;
}


function updateModelPerformance(reportList) {

    const latest = getLatestValidation(reportList);

    const mae = document.getElementById("model-mae-v5");
    const rmse = document.getElementById("model-rmse-v5");
    const mape = document.getElementById("model-mape-v5");
    const holdout = document.getElementById("model-holdout-v5");

    if (!latest) {
        if (mae) mae.textContent = "—";
        if (rmse) rmse.textContent = "—";
        if (mape) mape.textContent = "—";
        if (holdout) holdout.textContent = "—";
        return;
    }

    const v = latest.validation;

    if (mae) {
        mae.textContent =
            Number(v.mae || 0).toLocaleString("tr-TR", {
                maximumFractionDigits: 0
            });
    }

    if (rmse) {
        rmse.textContent =
            Number(v.rmse || 0).toLocaleString("tr-TR", {
                maximumFractionDigits: 0
            });
    }

    if (mape) {
        mape.textContent =
            "%" + Number(v.mape || 0).toLocaleString("tr-TR", {
                maximumFractionDigits: 2
            });
    }

    if (holdout) {
        holdout.textContent =
            String(v.holdout_months || "—") + " ay";
    }
}


async function loadInfrastructureV5() {

    try {

        const response = await authFetch("/api/v1/dashboard");
        const data = await response.json();

        const map = {
            "infra-redis-v5": data.redis,
            "infra-rabbitmq-v5": data.rabbitmq,
            "infra-minio-v5": data.minio,
            "infra-oracle-v5": data.oracle,
            "infra-worker-v5": data.analytics_worker
        };

        Object.entries(map).forEach(([id, value]) => {

            const element = document.getElementById(id);

            if (!element) {
                return;
            }

            let status = value;

            if (typeof value === "object" && value !== null) {
                status =
                    value.status ||
                    value.state ||
                    JSON.stringify(value);
            }

            status = String(status || "unknown").toUpperCase();

            element.textContent = status;

        });

    } catch (error) {

        console.error("Infrastructure load failed:", error);

    }
}


document.addEventListener("DOMContentLoaded", function() {

    setupDashboardNavigation();

    loadInfrastructureV5();

});



// ============================================================
// Dashboard V5 Model Metrics
// ============================================================

async function loadModelPerformanceV5() {

    try {

        const response = await authFetch("/api/v1/reports");

        if (!response.ok) {
            throw new Error("Reports request failed");
        }

        const data = await response.json();

        const reports =
            Array.isArray(data) ? data :
            Array.isArray(data.reports) ? data.reports :
            [];

        updateModelPerformance(reports);

    } catch (error) {

        console.error("Model performance load failed:", error);

    }
}


// Daha esnek altyapı durum okuyucu
async function refreshInfrastructureV5() {

    try {

        const response = await authFetch("/api/v1/dashboard");

        if (!response.ok) {
            throw new Error("Dashboard request failed");
        }

        const data = await response.json();

        function valueOf(name) {

            const candidates = [
                data[name],
                data.services && data.services[name],
                data.platform && data.platform[name],
                data.platform &&
                data.platform.services &&
                data.platform.services[name]
            ];

            for (const item of candidates) {

                if (item === undefined || item === null) {
                    continue;
                }

                if (typeof item === "string") {
                    return item;
                }

                if (typeof item === "object") {
                    return (
                        item.status ||
                        item.state ||
                        item.health ||
                        item.value ||
                        "UP"
                    );
                }

                return String(item);
            }

            return "UP";
        }


        const fields = {
            "infra-oracle-v5": valueOf("oracle"),
            "infra-redis-v5": valueOf("redis"),
            "infra-rabbitmq-v5": valueOf("rabbitmq"),
            "infra-minio-v5": valueOf("minio"),
            "infra-worker-v5": valueOf("analytics_worker")
        };


        Object.entries(fields).forEach(([id, value]) => {

            const element = document.getElementById(id);

            if (!element) {
                return;
            }

            element.textContent =
                String(value || "UNKNOWN").toUpperCase();

        });


    } catch (error) {

        console.error("Infrastructure refresh failed:", error);

    }

}


document.addEventListener("DOMContentLoaded", function() {

    loadModelPerformanceV5();
    refreshInfrastructureV5();

    setInterval(function() {
        loadModelPerformanceV5();
        refreshInfrastructureV5();
    }, 15000);

});



// ============================================================
// Dashboard V5 Polish Helpers
// ============================================================

function classifyStatusV5(value) {

    const s = String(value || "unknown").toLowerCase();

    if (
        s === "up" ||
        s === "operational" ||
        s === "active" ||
        s === "ready" ||
        s === "healthy"
    ) {
        return "ok";
    }

    if (
        s === "down" ||
        s === "failed" ||
        s === "error"
    ) {
        return "down";
    }

    if (
        s === "degraded" ||
        s === "warning"
    ) {
        return "warn";
    }

    return "unknown";
}


function applyInfrastructureBadgesV5() {

    const ids = [
        "infra-oracle-v5",
        "infra-redis-v5",
        "infra-rabbitmq-v5",
        "infra-minio-v5",
        "infra-worker-v5"
    ];

    ids.forEach(id => {

        const el = document.getElementById(id);

        if (!el) {
            return;
        }

        const value = el.textContent.trim();

        el.className =
            "service-status-v5 " +
            classifyStatusV5(value);

    });
}


async function loadOverviewMetricsV5() {

    try {

        const response = await authFetch("/api/v1/reports");

        if (!response.ok) {
            throw new Error("reports fetch failed");
        }

        const data = await response.json();

        const reports =
            Array.isArray(data) ? data :
            Array.isArray(data.reports) ? data.reports :
            [];

        const latest = getLatestValidation(reports);

        if (!latest) {
            return;
        }

        const v = latest.validation;

        const mae = document.getElementById("overview-mae-v5");
        const rmse = document.getElementById("overview-rmse-v5");
        const mape = document.getElementById("overview-mape-v5");
        const holdout = document.getElementById("overview-holdout-v5");

        if (mae) {
            mae.textContent =
                Number(v.mae || 0).toLocaleString("tr-TR", {
                    maximumFractionDigits: 0
                });
        }

        if (rmse) {
            rmse.textContent =
                Number(v.rmse || 0).toLocaleString("tr-TR", {
                    maximumFractionDigits: 0
                });
        }

        if (mape) {
            mape.textContent =
                "%" + Number(v.mape || 0).toLocaleString("tr-TR", {
                    maximumFractionDigits: 2
                });
        }

        if (holdout) {
            holdout.textContent =
                String(v.holdout_months || "—") + " ay";
        }

    } catch (error) {

        console.error("Overview metrics load failed:", error);

    }
}


document.addEventListener("DOMContentLoaded", function() {

    loadOverviewMetricsV5();

    setTimeout(applyInfrastructureBadgesV5, 1200);

    setInterval(function() {
        loadOverviewMetricsV5();
        setTimeout(applyInfrastructureBadgesV5, 800);
    }, 15000);

});



// ============================================================
// Reports Search / Filter / CSV
// ============================================================

let reportsCacheV5 = [];
let reportsPageV53 = 1;


function normalizeReportsV5(data) {

    if (Array.isArray(data)) {
        return data;
    }

    if (data && Array.isArray(data.reports)) {
        return data.reports;
    }

    return [];
}


function reportStatusV5(report) {

    return String(
        report.status ||
        (report.result && report.result.status) ||
        "completed"
    ).toLowerCase();
}


function filteredReportsV5() {

    const search =
        String(
            document.getElementById("report-search-v5")?.value || ""
        ).trim().toLowerCase();

    const status =
        String(
            document.getElementById("report-status-filter-v5")?.value || "all"
        ).toLowerCase();

    const startDate =
        String(
            document.getElementById("report-date-start-v53")?.value || ""
        );

    const endDate =
        String(
            document.getElementById("report-date-end-v53")?.value || ""
        );

    return reportsCacheV5.filter(report => {

        const haystack = [
            report.message_id,
            report.task_type,
            report.status,
            report.created_at,
            report.result && report.result.method,
            report.result && report.result.source_start,
            report.result && report.result.source_end
        ]
        .filter(Boolean)
        .join(" ")
        .toLowerCase();

        const searchOk =
            !search || haystack.includes(search);

        const statusOk =
            status === "all" ||
            reportStatusV5(report) === status;

        let dateOk = true;

        if (report.created_at) {

            const created =
                new Date(report.created_at);

            if (startDate) {
                const start =
                    new Date(startDate + "T00:00:00");

                if (created < start) {
                    dateOk = false;
                }
            }

            if (endDate) {
                const end =
                    new Date(endDate + "T23:59:59");

                if (created > end) {
                    dateOk = false;
                }
            }
        }

        return searchOk && statusOk && dateOk;
    });
}


function renderReportsFilteredV5() {

    const filtered =
        filteredReportsV5();

    const root =
        document.getElementById("reports-table");

    const count =
        document.getElementById("reports-count-v5");

    const pageSize =
        Number(
            document.getElementById("report-page-size-v53")?.value || 10
        );

    const total =
        filtered.length;

    const totalPages =
        Math.max(
            1,
            Math.ceil(total / pageSize)
        );

    if (reportsPageV53 > totalPages) {
        reportsPageV53 = totalPages;
    }

    if (reportsPageV53 < 1) {
        reportsPageV53 = 1;
    }

    const startIndex =
        (reportsPageV53 - 1) * pageSize;

    const pageReports =
        filtered.slice(
            startIndex,
            startIndex + pageSize
        );

    if (count) {
        count.textContent =
            total + " rapor filtreyle eşleşiyor";
    }

    const info =
        document.getElementById(
            "reports-pagination-info-v53"
        );

    if (info) {

        if (total === 0) {
            info.textContent = "0 rapor";
        } else {
            info.textContent =
                (startIndex + 1) +
                " - " +
                Math.min(startIndex + pageSize, total) +
                " / " +
                total;
        }
    }

    const pageNumber =
        document.getElementById(
            "reports-page-number-v53"
        );

    if (pageNumber) {
        pageNumber.textContent =
            "Sayfa " +
            reportsPageV53 +
            " / " +
            totalPages;
    }

    const prev =
        document.getElementById(
            "reports-prev-v53"
        );

    const next =
        document.getElementById(
            "reports-next-v53"
        );

    if (prev) {
        prev.disabled =
            reportsPageV53 <= 1;
    }

    if (next) {
        next.disabled =
            reportsPageV53 >= totalPages;
    }

    if (!root) {
        return;
    }

    if (!pageReports.length) {

        root.innerHTML =
            '<div class="empty">' +
            'Filtreye uygun rapor bulunamadı.' +
            '</div>';

        return;
    }

    const rows =
        pageReports.map(report => {

        const result =
            report.result || {};

        const id =
            String(
                report.message_id || ""
            );

        const shortId =
            id
              ? id.slice(0, 13)
              : "—";

        const method =
            String(
                result.method || "—"
            );

        const created =
            report.created_at
              ? new Date(
                    report.created_at
                ).toLocaleString("tr-TR")
              : "—";

        const statusRaw =
            reportStatusV5(report);

        const statusText =
            statusRaw === "completed"
              ? "COMPLETED"
              : statusRaw === "failed"
                ? "FAILED"
                : statusRaw.toUpperCase();

        const badgeClass =
            statusRaw === "completed"
              ? "completed"
              : statusRaw === "failed"
                ? "failed"
                : "other";

        return (
            '<tr style="cursor:pointer" ' +
            'data-id="' + id + '" ' +
            'onclick="showReport(this.dataset.id)">' +

            '<td>' + shortId + '</td>' +

            '<td>' +
            String(report.task_type || "—") +
            '</td>' +

            '<td>' +
            method +
            '</td>' +

            '<td>' +
            created +
            '</td>' +

            '<td>' +
            '<span class="report-status-badge-v53 ' +
            badgeClass +
            '">' +
            statusText +
            '</span>' +
            '</td>' +

            '</tr>'
        );

    }).join("");

    root.innerHTML =
        '<table>' +
        '<thead>' +
        '<tr>' +
        '<th>Görev ID</th>' +
        '<th>Tür</th>' +
        '<th>Model</th>' +
        '<th>Oluşturulma</th>' +
        '<th>Durum</th>' +
        '</tr>' +
        '</thead>' +
        '<tbody>' +
        rows +
        '</tbody>' +
        '</table>';
}


function previousReportsPageV53() {

    if (reportsPageV53 > 1) {
        reportsPageV53 -= 1;
        renderReportsFilteredV5();
    }
}


function nextReportsPageV53() {

    const pageSize =
        Number(
            document.getElementById("report-page-size-v53")?.value || 10
        );

    const totalPages =
        Math.max(
            1,
            Math.ceil(
                filteredReportsV5().length /
                pageSize
            )
        );

    if (reportsPageV53 < totalPages) {
        reportsPageV53 += 1;
        renderReportsFilteredV5();
    }
}


async function loadReportsToolsV5() {

    try {

        const response =
            await authFetch("/api/v1/reports");

        if (!response.ok) {
            throw new Error("reports fetch failed");
        }

        const data =
            await response.json();

        reportsCacheV5 =
            normalizeReportsV5(data);

        renderReportsFilteredV5();

    } catch (error) {

        console.error(
            "Reports tools load failed:",
            error
        );

    }
}


function csvEscapeV5(value) {

    const text =
        String(
            value === undefined ||
            value === null
              ? ""
              : value
        );

    return '"' +
        text.replace(/"/g, '""') +
        '"';
}


function exportReportsCsvV5() {

    const reports =
        filteredReportsV5();

    if (!reports.length) {
        alert("Dışa aktarılacak rapor bulunamadı.");
        return;
    }

    const rows = [
        [
            "message_id",
            "task_type",
            "status",
            "created_at",
            "model",
            "source_start",
            "source_end",
            "source_months",
            "mae",
            "rmse",
            "mape",
            "holdout_months"
        ]
    ];

    reports.forEach(report => {

        const result =
            report.result || {};

        const validation =
            result.validation || {};

        rows.push([
            report.message_id || "",
            report.task_type || "",
            report.status || "",
            report.created_at || "",
            result.method || "",
            result.source_start || "",
            result.source_end || "",
            result.source_months || "",
            validation.mae || "",
            validation.rmse || "",
            validation.mape || "",
            validation.holdout_months || ""
        ]);

    });

    const csv =
        rows
        .map(row =>
            row.map(csvEscapeV5).join(";")
        )
        .join("\\n");

    const blob =
        new Blob(
            ["\\uFEFF" + csv],
            {type:"text/csv;charset=utf-8;"}
        );

    const url =
        URL.createObjectURL(blob);

    const a =
        document.createElement("a");

    a.href = url;
    a.download =
        "oracle-university-reports-" +
        new Date().toISOString().slice(0,10) +
        ".csv";

    document.body.appendChild(a);
    a.click();
    a.remove();

    URL.revokeObjectURL(url);
}


document.addEventListener("DOMContentLoaded", function() {

    const search =
        document.getElementById("report-search-v5");

    const status =
        document.getElementById("report-status-filter-v5");

    if (search) {
        search.addEventListener(
            "input",
            renderReportsFilteredV5
        );
    }

    if (status) {
        status.addEventListener(
            "change",
            renderReportsFilteredV5
        );
    }

    loadReportsToolsV5();

    setInterval(
        loadReportsToolsV5,
        15000
    );

});



// ============================================================
// Dashboard V5.3 Report Filters
// ============================================================

document.addEventListener("DOMContentLoaded", function() {

    const ids = [
        "report-search-v5",
        "report-status-filter-v5",
        "report-date-start-v53",
        "report-date-end-v53",
        "report-page-size-v53"
    ];

    ids.forEach(id => {

        const element =
            document.getElementById(id);

        if (!element) {
            return;
        }

        const eventName =
            element.tagName === "INPUT"
              ? "input"
              : "change";

        element.addEventListener(
            eventName,
            function() {
                reportsPageV53 = 1;
                renderReportsFilteredV5();
            }
        );

    });

});



// ============================================================
// Dashboard Analytics V6
// ============================================================

async function loadModelComparisonV6() {

    try {

        const response = await authFetch("/api/v1/reports");

        if (!response.ok) {
            return;
        }

        const data = await response.json();

        const reports =
            Array.isArray(data)
              ? data
              : Array.isArray(data.reports)
                ? data.reports
                : [];

        const report = reports.find(item =>
            item &&
            item.result &&
            item.result.model_comparison
        );

        if (!report) {
            return;
        }

        const comparison =
            report.result.model_comparison || {};

        const sarimax =
            comparison.sarimax &&
            comparison.sarimax.validation
              ? comparison.sarimax.validation
              : {};

        const baseline =
            comparison.seasonal_naive &&
            comparison.seasonal_naive.validation
              ? comparison.seasonal_naive.validation
              : {};

        const sarimaxEl =
            document.getElementById("v6-sarimax-mape");

        const baselineEl =
            document.getElementById("v6-baseline-mape");

        const bestEl =
            document.getElementById("v6-best-model");

        if (sarimaxEl) {
            sarimaxEl.textContent =
                sarimax.mape !== undefined
                  ? "%" + Number(sarimax.mape).toLocaleString(
                      "tr-TR",
                      {maximumFractionDigits:2}
                    )
                  : "—";
        }

        if (baselineEl) {
            baselineEl.textContent =
                baseline.mape !== undefined
                  ? "%" + Number(baseline.mape).toLocaleString(
                      "tr-TR",
                      {maximumFractionDigits:2}
                    )
                  : "—";
        }

        if (bestEl) {

            const best =
                String(
                    comparison.best_model || "—"
                );

            bestEl.textContent =
                best === "sarimax"
                  ? "SARIMAX"
                  : best === "seasonal_naive"
                    ? "Seasonal Naive"
                    : best;
        }

    } catch (error) {
        console.error(
            "V6 model comparison failed:",
            error
        );
    }
}


document.addEventListener(
    "DOMContentLoaded",
    function() {

        loadModelComparisonV6();

        setInterval(
            loadModelComparisonV6,
            15000
        );
    }
);


</script>
</body>
</html>
"""

@app.get("/dashboard", response_class=HTMLResponse, include_in_schema=False)
def web_dashboard():
    return DASHBOARD_HTML

