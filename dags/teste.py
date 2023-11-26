from airflow import models
import os
from airflow.operators.bash_operator import BashOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.providers.google.cloud.operators.kubernetes_engine import (
    GKECreateClusterOperator,
    GKEDeleteClusterOperator,
    GKEStartPodOperator,
)
from airflow.utils.dates import days_ago

with models.DAG(
    "example_gcp_gke",
    schedule_interval=None,  # Override to match your needs
    start_date=days_ago(1),
    tags=["example"],
) as dag:
    # TODO(developer): update with your values
    PROJECT_ID = "dagdependency"
    CLUSTER_ZONE = "us-central1-c"
    CLUSTER_NAME = "example-cluster"
    # CLUSTER = {"name": CLUSTER_NAME, "initial_node_count": 1}

    CLUSTER = {
        "name": CLUSTER_NAME,
        "node_pools": [
        {
            "name": "pool-0",
            "initial_node_count": 3,
            "config": {
                "machineType": "n1-standard-1",  # Tipo de máquina desejado
                "preemptible": True,  # Usar ou não máquinas preemptíveis
                "diskSizeGb": 10,  # Capacidade do disco em GB
            }
        }
    ],
    }

    create_cluster = GKECreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        location=CLUSTER_ZONE,
        body=CLUSTER,
    )
    
    kubernetes_min_pod = GKEStartPodOperator(
        task_id="ex-kube-templates",
        name="ex-kube-templates",
        project_id=PROJECT_ID,
        location=CLUSTER_ZONE,
        cluster_name=CLUSTER_NAME,
        namespace="default",
        image="gcr.io/dagdependency/teste/teste:1.0",
        cmds=[
        "sh",
        "-c",
        " echo 'vai autenticar' && gcloud auth activate-service-account --key-file=/dagdependency-9dc6252e7cfc.json && echo 'autenticou' && echo 'instalando plugins' && gcloud components install gke-gcloud-auth-plugin && echo 'instalou plugins' && echo 'pegando credencial' && gcloud container clusters get-credentials example-cluster --zone us-central1-c --project dagdependency && kubectl get nodes && kubectl get pods"
        ],
        gcp_conn_id='google_cloud_default',
        impersonation_chain="306212353875-compute@developer.gserviceaccount.com",
        deferrable=False
    )
    
    # delete_cluster = GKEDeleteClusterOperator(
    #     task_id="delete_cluster",
    #     name=CLUSTER_NAME,
    #     project_id=PROJECT_ID,
    #     location=CLUSTER_ZONE,
    # )

    # create_cluster >> create_node_pools >> kubernetes_min_pod >> delete_cluster
    create_cluster >> kubernetes_min_pod # >> delete_cluster
    # create_cluster >> 
    # kubernetes_min_pod # >> delete_cluster
