# 🚚 SuperCourier - Mini ETL Pipeline

SuperCourier est un mini pipeline **ETL (Extract, Transform, Load)** construit en Python. Il simule une activité de livraisons avec prise en compte de multiples facteurs : type de colis 📦, zone géographique 🗺️, météo 🌦️ et horaires 🕒. L'objectif est de générer des données synthétiques de livraisons, les enrichir, calculer les temps de livraison, puis exporter les résultats en CSV 📄.

---

## ✨ Fonctionnalités

✅ Génération de 1000 livraisons aléatoires sur 3 mois
✅ Base SQLite locale pour les livraisons
✅ Données météo heure par heure 🌤️
✅ Calcul du temps de livraison théorique et réel 🧠
✅ Détection automatique des retards ⏱️
✅ Export des données et des statistiques en CSV 📊

---

## 🗂️ Structure du projet

```
.
├── supercourier_etl.py         # Script principal
├── output_data/
│   ├── deliveries.csv          # Données finales enrichies
│   ├── statistics.csv          # Statistiques globales
│   └── weather_data.json       # Données météo simulées
├── supercourier_mini.db        # Base SQLite
├── info.log                    # Logs d'exécution
```

---

## 🚀 Installation et Exécution

### 🔧 Prérequis

* Python 3.8+ 🐍
* pip installé 💡

### 🛠️ Installation locale

```bash
# Cloner le projet
git clone <url-du-repo>
cd supercourier

# Créer un environnement virtuel
python -m venv venv
source venv/bin/activate        # Linux/macOS
venv\Scripts\activate           # Windows

# Installer les dépendances
pip install -r requirements.txt
```

### ▶️ Lancer le pipeline

```bash
python supercourier_etl.py
```

---

## 🐳 Utilisation avec Docker

### 🧱 Créer l'image Docker

```bash
docker build -t supercourier-etl .
```

### 📦 Exécuter le conteneur

```bash
# Unix/Linux/MacOS
docker run --rm -v $(pwd)/output_data:/app/output_data supercourier-etl

# PowerShell (Windows)
docker run --rm -v ${PWD}/output_data:/app/output_data supercourier-etl
```

---

## 🧩 Modules principaux du pipeline

### 🔹 `create_sqlite_database()`

Génère une base SQLite avec 1000 livraisons simulées 📅

### 🔹 `generate_weather_data()`

Crée un fichier JSON contenant la météo heure par heure sur 3 mois 🌦️

### 🔹 `extract_sqlite_data()` & `load_weather_data()`

Chargent les données de livraison et météo dans des DataFrames 🗃️

### 🔹 `transform_data()`

Fusion, enrichissement, calculs et marquage des statuts 🚦

### 🔹 `save_results()`

Sauvegarde CSV des résultats et export des statistiques 📈

---

## 📄 Exemple de sorties

* `deliveries.csv` : Données enrichies (type, distance, météo, temps, statut)
* `statistics.csv` : Délai moyen, taux de retard, min/max
* `info.log` : Fichier de logs détaillés 📋


