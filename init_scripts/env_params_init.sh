ENV_CONTENT="AIRFLOW_UID=50000\nAIRFLOW_GID=0"
echo -e "$ENV_CONTENT" | sudo tee .env >/dev/null
