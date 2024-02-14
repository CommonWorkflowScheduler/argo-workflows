#!/bin/sh

kubectl apply -f serviceaccount.yaml
kubectl apply -f clusterrole.yaml
kubectl apply -f clusterrolebinding.yaml
kubectl apply -f cws.yaml
kubectl apply -f service.yaml
kubectl wait --for=condition=ready --timeout=-1s pod workflow-scheduler
kubectl port-forward --address 0.0.0.0 workflow-scheduler 8081:8080

