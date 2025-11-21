FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Dépendances Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Scripts
COPY producer.py consumer.py ./

# Par défaut on laisse la commande surchargée par docker-compose
CMD ["bash"]

