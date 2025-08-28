# MLflow Deployment with Kubernetes

## 1️⃣ Démarrer la base PostgreSQL
```bash
docker compose up -d




# Create deployment for jedha/sample-mlflow-server
kubectl apply -f deployment-resources-mlflow.yaml


# vérifier si le deployment s'est bien passé 

kubectl rollout status deployments/sample-mlflow-server


# exposer le service à l'exterieur du cluster minikube : 

minikube service sample-mlflow-server
