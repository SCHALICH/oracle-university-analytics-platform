# Oracle University Analytics Platform

Oracle SH satış verilerini kullanan, asenkron tahmin üretimi ve raporlama sağlayan Kubernetes tabanlı analitik platform.

## Temel Akış

Browser
→ Nginx
→ FastAPI
→ RabbitMQ
→ Analytics Worker
→ Oracle SH.SALES
→ Tahmin Modelleri
→ MinIO
→ Dashboard

## Kullanılan Teknolojiler

- Oracle Database 26ai Free
- FastAPI
- Python
- Pandas
- Statsmodels / SARIMAX
- RabbitMQ
- Redis
- MinIO
- Nginx
- Kubernetes / K3s
- Prometheus
- Grafana
- Podman
- Helm

## Analytics

Platform şu anda iki modeli karşılaştırır:

- SARIMAX(1,1,1)(1,1,1,12)
- Seasonal Naive

Model karşılaştırması 6 aylık holdout doğrulama kümesi üzerinde yapılır.

Kullanılan metrikler:

- MAE
- RMSE
- MAPE

En uygun model MAPE değerine göre seçilir.

Tahmin periyodu:

- 3 ay
- 6 ay
- 12 ay

## Dashboard

Canlı dashboard:

http://192.168.56.102:30080/dashboard

Dashboard bölümleri:

- Genel Bakış
- Satış Analitiği
- Tahmin Modelleri
- Raporlar
- Altyapı

## Observability

Prometheus:

http://192.168.56.102:30090

Grafana:

http://192.168.56.102:30030

Grafana üzerinde:

- Kubernetes servis durumu
- Pod / deployment durumu
- API request metrikleri
- API response süreleri
- Worker uptime
- Forecast metrikleri

izlenmektedir.

## Kubernetes Namespace

university-platform

## Aktif API

oracle-university-api-v2

## Analytics Worker

analytics-worker

## Proje Durumu

Platform K3s üzerinde çalışır durumdadır.

Oracle veritabanından gerçek satış verisi okunmakta, analitik worker tarafından tahmin oluşturulmakta ve sonuçlar dashboard üzerinden raporlanmaktadır.

## CI/CD

Projenin CI/CD süreci GitHub Actions ve DevOps-Lab üzerinde çalışan
self-hosted runner ile yürütülmektedir.

Üretim dağıtım akışı:

```text
Geliştirici
   |
   v
Git Push
   |
   v
GitHub
   |
   +--> CI Kontrolleri
   |     |
   |     +--> Python sözdizimi kontrolü
   |     +--> Gerekli dosyaların kontrolü
   |
   v
Manuel Üretim Dağıtımı
   |
   v
GitHub production ortamı
   |
   v
Self-hosted GitHub Actions Runner
   |
   v
Podman image build
   |
   v
K3s image import
   |
   v
Kubernetes rollout
   |
   v
Uygulama health check
   |
   +--> Başarılı --> Git tag + GitHub Release
   |
   +--> Hata --> Kubernetes rollback

