# Airflow 2 Helm chart
Dataops-sre's Airflow2 public Helm charts repository

## TL;TR
To install charts in this repository with command line:

```bash
helm repo add dataops-sre-airflow https://dataops-sre.github.io/docker-airflow2/
helm repo update
helm install airflow dataops-sre-airflow/airflow --wait --timeout 300s
```

with terraform:
```yaml
resource "helm_release" "aiflow" {
  name       = "airflow"
  repository = "https://dataops-sre.github.io/docker-airflow2/"
  chart      = "airflow"
  namespace  = "default"
}
```
