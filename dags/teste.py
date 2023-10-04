from airflow import models
from airflow.operators.bash_operator import BashOperator
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
    PROJECT_ID = "airflowgke-398222"
    CLUSTER_ZONE = "us-central1-c"
    CLUSTER_NAME = "example-cluster"
    # CLUSTER = {"name": CLUSTER_NAME, "initial_node_count": 1}

    CLUSTER = {
        "name": CLUSTER_NAME,
        "node_pools": [
            {"name": "pool-0", "initial_node_count": 1}
        ],
    }

    create_cluster = GKECreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        location=CLUSTER_ZONE,
        body=CLUSTER,
    )
    # create_cluster = GKECreateClusterOperator(
    #     task_id="create_cluster",
    #     project_id=PROJECT_ID,
    #     location=CLUSTER_ZONE,
    #     body=CLUSTER,
    # )
    # # Using the BashOperator to create node pools is a workaround
    # # In Airflow 2, because of https://github.com/apache/airflow/pull/17820
    # # Node pool creation can be done using the GKECreateClusterOperator

    # create_node_pools = BashOperator(
    #     task_id="create_node_pools",
    #     bash_command=f"gcloud container node-pools create pool-0 \
    #                     --cluster {CLUSTER_NAME} \
    #                     --num-nodes 1 \
    #                     --zone {CLUSTER_ZONE} \
    #                     && gcloud container node-pools create pool-1 \
    #                     --cluster {CLUSTER_NAME} \
    #                     --num-nodes 1 \
    #                     --zone {CLUSTER_ZONE}",
    # )

    # kubernetes_min_pod = GKEStartPodOperator(
    #     # The ID specified for the task.
    #     task_id="pod-ex-minimum",
    #     # Name of task you want to run, used to generate Pod ID.
    #     name="pod-ex-minimum",
    #     project_id=PROJECT_ID,
    #     location=CLUSTER_ZONE,
    #     cluster_name=CLUSTER_NAME,
    #     # Entrypoint of the container, if not specified the Docker container's
    #     # entrypoint is used. The cmds parameter is templated.
    #     cmds=["echo"],
    #     # The namespace to run within Kubernetes, default namespace is
    #     # `default`.
    #     namespace="default",
    #     # Docker image specified. Defaults to hub.docker.com, but any fully
    #     # qualified URLs will point to a custom repository. Supports private
    #     # gcr.io images if the Composer Environment is under the same
    #     # project-id as the gcr.io images and the service account that Composer
    #     # uses has permission to access the Google Container Registry
    #     # (the default service account has permission)
    #     image="gcr.io/gcp-runtimes/ubuntu_18_0_4",
    # )

    delete_cluster = GKEDeleteClusterOperator(
        task_id="delete_cluster",
        name=CLUSTER_NAME,
        project_id=PROJECT_ID,
        location=CLUSTER_ZONE,
    )

    # create_cluster >> create_node_pools >> kubernetes_min_pod >> delete_cluster
    # create_cluster >> kubernetes_min_pod >> delete_cluster
    create_cluster >> delete_cluster