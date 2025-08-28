# démarrer la bdd 

docker compose up -d



# Create deployment for jedha/sample-mlflow-server
kubectl apply -f .\deployment-resources-mlflow.yaml -n nsam

# vérifier si le deployment s'est bien passé 

kubectl rollout status deployments/sample-mlflow-server -n nsam

# exposer le service à l'exterieur du cluster minikube : 

minikube service sample-mlflow-server -n nsam