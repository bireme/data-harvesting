import csv
import json
import logging
import os
from collections import defaultdict
from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook


def export_issn_records():
    logger = logging.getLogger("airflow.task")

    file_export_path = Variable.get("OAI_DC_INPUT_PATH")
    issn_export_path = os.path.join(file_export_path, "issn")

    os.makedirs(issn_export_path, exist_ok=True)

    csv_file_path = os.path.join(
        issn_export_path,
        "issn.csv",
    )

    logger.info("Diretório de exportação: %s", issn_export_path)
    logger.info("Arquivo CSV: %s", csv_file_path)

    mongo_hook = MongoHook(mongo_conn_id="mongo")
    client = mongo_hook.get_conn()

    db = client["TITLE"]
    collection = db["portal_issn"]

    total_records = collection.count_documents({})

    logger.info(
        "Total de registros encontrados: %d",
        total_records,
    )

    try:
        # ---------------------------------------------------------
        # identificar todos os campos existentes
        # ---------------------------------------------------------

        field_columns = set()

        cursor = collection.find({})

        for record in cursor:
            field_occurrences = defaultdict(int)

            for field in record.get("fields", []):
                if not field:
                    continue

                field_tag = next(iter(field))

                field_occurrences[field_tag] += 1

                occurrence = field_occurrences[field_tag]

                if occurrence == 1:
                    column_name = field_tag
                else:
                    column_name = f"{field_tag}_{occurrence}"

                field_columns.add(column_name)

        # ---------------------------------------------------------
        # Ordenar os campos numericamente
        # ---------------------------------------------------------

        def field_sort_key(column):
            parts = column.split("_")

            try:
                field_number = int(parts[0])
            except ValueError:
                field_number = 99999

            occurrence = (
                int(parts[1])
                if len(parts) > 1 and parts[1].isdigit()
                else 1
            )

            return field_number, occurrence

        sorted_field_columns = sorted(
            field_columns,
            key=field_sort_key,
        )

        csv_columns = [
            "issn",
            "leader",
        ] + sorted_field_columns

        logger.info(
            "Total de colunas identificadas: %d",
            len(csv_columns),
        )

        logger.info(
            "Colunas: %s",
            ", ".join(csv_columns),
        )

        # ---------------------------------------------------------
        # exportar os registros
        # ---------------------------------------------------------

        exported = 0
        skipped = 0

        with open(
            csv_file_path,
            "w",
            encoding="utf-8",
            newline="",
        ) as csv_file:

            writer = csv.DictWriter(
                csv_file,
                fieldnames=csv_columns,
                extrasaction="ignore",
            )
            writer.writeheader()
            cursor = collection.find({})

            for record in cursor:
                issn = record.get("issn")
                if not issn:
                    skipped += 1

                    logger.warning(
                        "Registro %s ignorado porque não possui ISSN.",
                        record.get("_id"),
                    )

                    continue

                row = {
                    "issn": issn,
                    "leader": record.get("leader", ""),
                }

                field_occurrences = defaultdict(int)

                for field in record.get("fields", []):
                    if not field:
                        continue

                    field_tag = next(iter(field))
                    field_value = field[field_tag]

                    field_occurrences[field_tag] += 1

                    occurrence = field_occurrences[field_tag]

                    if occurrence == 1:
                        column_name = field_tag
                    else:
                        column_name = f"{field_tag}_{occurrence}"

                    if isinstance(field_value, str):
                        row[column_name] = field_value

                    elif isinstance(field_value, dict):
                        row[column_name] = json.dumps(
                            field_value,
                            ensure_ascii=False,
                        )

                    else:
                        row[column_name] = str(field_value)

                writer.writerow(row)

                exported += 1

                if exported % 1000 == 0:
                    logger.info(
                        "Registros exportados: %d",
                        exported,
                    )

        logger.info(
            "Exportação concluída. "
            "Exportados: %d | Ignorados: %d | Total: %d",
            exported,
            skipped,
            total_records,
        )

    finally:
        client.close()


default_args = {
    "owner": "airflow",
    "retries": 0,
}

with DAG(
    dag_id="DH_02_export_issn",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["mongodb", "issn", "export"],
) as dag:

    export_issn_task = PythonOperator(
        task_id="export_issn_task",
        python_callable=export_issn_records,
    )