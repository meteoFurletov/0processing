from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator


@dag(
    dag_id="rabbitmq_s3_consumer_taskflow",
    description="DAG to run RabbitMQ S3 Consumer using TaskFlow API",
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["rabbitmq", "consumer"],
    default_args={
        "owner": "airflow",
        "retries": 2,
    },
)
def rabbitmq_consumer_dag():

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
        """This task runs the RabbitMQ consumer in a Kubernetes pod"""
        # This function body won't be executed directly, it's just a placeholder
        # The actual execution happens in the container defined in the decorator
        pass

    # Run the task
    run_rabbitmq_consumer()


# Instantiate the DAG
dag = rabbitmq_consumer_dag()
