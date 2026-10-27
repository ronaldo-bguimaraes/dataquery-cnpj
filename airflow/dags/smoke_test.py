"""DAG manual para confirmar que o Airflow consegue executar uma tarefa."""

from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator


def contar_ate_tres() -> None:
    """Faz um trabalho simples e registra o resultado no log da tarefa."""
    numeros = list(range(1, 4))
    print(f"Airflow funcionando! Contagem: {numeros}; soma: {sum(numeros)}")


with DAG(
    dag_id="smoke_test_manual",
    description="Teste manual e inofensivo da execução de uma tarefa no Airflow.",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    tags=["teste", "manual"],
) as dag:
    PythonOperator(
        task_id="contar_ate_tres",
        python_callable=contar_ate_tres,
    )
