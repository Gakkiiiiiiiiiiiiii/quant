FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
COPY configs ./configs
RUN pip install --no-cache-dir ".[api]"
EXPOSE 8011
# §65：quant 固定 8011 端口，同时承载旧 Dashboard API 与 market-data.v1/backtest.v1/trading.v1 契约 API。
CMD ["python", "-c", "from quant_demo.api.app import serve; serve('0.0.0.0', 8011)"]
