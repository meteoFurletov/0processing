from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator


@dag(
    dag_id="rabbitmq_s3_consumer_taskflow",
    description="RabbitMQ to S3/MinIO consumer that processes messages from a queue and stores them as JSON files",
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["rabbitmq", "consumer", "s3", "minio"],
    default_args={
        "owner": "airflow",
        "retries": 2,
    },
)
def rabbitmq_consumer_dag():
    """
    ### RabbitMQ to S3/MinIO Consumer DAG

    This DAG runs a consumer that:
    - Connects to a RabbitMQ server
    - Consumes messages from a specified queue
    - Uploads each message as a JSON file to an S3-compatible storage (MinIO)
    - Handles acknowledgments, retries, and error cases

    The consumer runs as a long-lived process in a Kubernetes pod and will continue
    processing messages until manually terminated.
    """

    @task.kubernetes(
        image="your-registry/rabbitmq-s3-consumer:latest",
        name="rabbitmq-s3-consumer",
        namespace="default",
        get_logs=True,
        log_events_on_failure=True,
        execution_timeout=None,
        delete_pod_on_completion=True,
        in_cluster=True,
        resources={
            "request_cpu": "100m",
            "request_memory": "256Mi",
            "limit_cpu": "200m",
            "limit_memory": "512Mi",
        },
    )
    def run_rabbitmq_consumer():
        """
        Run the RabbitMQ consumer in a Kubernetes pod.

        This task deploys a container that:
        - Establishes a connection to RabbitMQ using environment variables
        - Sets up a connection to MinIO/S3 storage
        - Processes messages, converting them to JSON files
        - Handles message acknowledgment based on successful processing

        The consumer will run indefinitely until the pod is terminated.
        """
        # This function body won't be executed directly, it's just a placeholder
        # The actual execution happens in the container defined in the decorator
        pass

    # Run the consumer task
    run_rabbitmq_consumer()


# Instantiate the DAG
dag = rabbitmq_consumer_dag()
