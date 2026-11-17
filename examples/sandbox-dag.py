from datetime import datetime, timezone

from airflow.sdk import DAG, task


with DAG("mcp_validation", schedule=None, start_date=datetime(2026, 1, 1, tzinfo=timezone.utc), catchup=False):
    @task
    def upstream():
        import time
        time.sleep(2)

    @task
    def retry_me():
        import time
        from airflow.sdk import get_current_context
        time.sleep(2)
        if get_current_context()["ti"].try_number == 1:
            raise ValueError("Synthetic first-attempt failure")

    @task
    def downstream():
        import time
        time.sleep(2)

    @task
    def unrelated():
        return "independent"

    upstream() >> retry_me() >> downstream()
    unrelated()
