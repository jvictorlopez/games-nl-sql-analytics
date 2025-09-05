FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# copy code
COPY app /app/app
# copy CSV into /app/data at build-time if present at context root
COPY base_jogos.csv /app/data/base_jogos.csv
ENV GAMES_CSV_PATH=/app/data/base_jogos.csv

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]

