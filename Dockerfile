FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir ionis-mcp beautifulsoup4 requests
RUN mkdir -p /root/.ionis-mcp/data/
EXPOSE 8000

ENTRYPOINT ["python", "-m", "ionis_mcp.server"]
CMD ["--port", "8000", "--transport", "streamable-http"]