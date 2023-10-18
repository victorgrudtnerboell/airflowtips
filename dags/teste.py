from airflow import models
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
    PROJECT_ID = "airflowgke-402322"
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

    # create_cluster = GKECreateClusterOperator(
    #     task_id="create_cluster",
    #     project_id=PROJECT_ID,
    #     location=CLUSTER_ZONE,
    #     body=CLUSTER,
    # )

    # kubernetes_min_pod = GKEStartPodOperator(
    #     task_id="pod-ex-minimum",
    #     name="pod-ex-minimum",
    #     project_id=PROJECT_ID,
    #     location=CLUSTER_ZONE,
    #     cluster_name=CLUSTER_NAME,
    #     cmds=["echo"],
    #     namespace="default",
    #     image="gcr.io/gcp-runtimes/ubuntu_18_0_4",
    # )

    kubernetes_pod = KubernetesPodOperator(
        task_id="kubernetes_pod",
        namespace='default',
        image="ubuntu:latest",
        cmds=["bash", "-cx"],
        arguments=["for i in {1..10}; do echo -n 'Olá mundo '; done;"],
        name="kubernetes-pod",
        is_delete_operator_pod=True,
        hostnetwork=False,
        startup_timeout_seconds=1000
        kube_conn_id="teste"
    )
    
    # delete_cluster = GKEDeleteClusterOperator(
    #     task_id="delete_cluster",
    #     name=CLUSTER_NAME,
    #     project_id=PROJECT_ID,
    #     location=CLUSTER_ZONE,
    # )

    # create_cluster >> create_node_pools >> kubernetes_min_pod >> delete_cluster
    # create_cluster >> kubernetes_min_pod >> delete_cluster
    # create_cluster >> 
    kubernetes_pod # >> delete_cluster
