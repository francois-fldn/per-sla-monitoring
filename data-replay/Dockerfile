# On part d'une image Python légère mais 100% compatible
FROM python:3.12-slim

# On empêche Python de créer des fichiers .pyc inutiles et on force l'affichage des logs
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# On crée un dossier de travail dans le conteneur
WORKDIR /app

# On copie d'abord les dépendances (pour optimiser le cache de Docker)
COPY requirements.txt .

# On installe les bibliothèques (comme paho-mqtt)
RUN pip install --no-cache-dir -r requirements.txt

# On copie le reste de notre code
COPY . .

# On lance le programme (Remplace par "script_agent.py" pour le 2ème Dockerfile)
CMD ["python", "data-replay.py"]