#  Supercourier Mini ETL

Un mini pipeline ETL en Python qui simule l'extraction, la transformation et le chargement de données de livraisons. Le but est de créer une base SQLite locale et d'exporter les données nettoyées au format CSV.  

Ce projet contient un pipeline ETL complet en Python. Il commence par la génération de données réalistes :

des données de livraisons, stockées dans une base SQLite (supercourier_mini.db),

et des données météo associées, sauvegardées dans un fichier JSON.

Ensuite, un processus ETL (Extract, Transform, Load) est exécuté :
il extrait les données des deux sources, les fusionne intelligemment, applique des transformations (nettoyage, enrichissement, formatage), puis les exporte dans un fichier CSV final (cleaned_data.csv) prêt pour analyse ou exploitation.

##  Lancer le projet projet

Créer un environnement virtuel Python 

```bash
python -m venv venv
source venv/bin/activate     
venv\Scripts\activate  

pip install -r requirements.txt

python supercourier_etl.py
```

## Construire l'image Docker 
```bash
docker build -t supercourier-etl .

docker run --rm -v $(pwd)/output_data:/app/output_data supercourier-etl

powershell : 

docker run --rm -v ${PWD}/output_data:/app/output_data supercourier-etl
```