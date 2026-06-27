FROM python:3.12-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY sync_facturas.py sync_productos.py sync_meta.py service.py notifier.py ./

EXPOSE 8000

# Liveness: el orquestador reinicia el contenedor si /health no responde.
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
  CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://localhost:8000/health').getcode()==200 else 1)"

CMD ["uvicorn", "service:app", "--host", "0.0.0.0", "--port", "8000"]
