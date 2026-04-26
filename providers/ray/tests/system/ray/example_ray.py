# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Example DAG demonstrating Ray provider operators and decorator."""

from __future__ import annotations

from datetime import datetime

# [START howto_operator_ray_setup_teardown]
from airflow import DAG
from airflow.providers.ray.operators.ray import DeleteRayCluster, SetupRayCluster, SubmitRayJob

with DAG(
    dag_id="example_ray_operator",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "ray"],
) as dag:
    setup_cluster = SetupRayCluster(
        task_id="setup_ray_cluster",
        conn_id="ray_default",
        ray_cluster_yaml="/opt/airflow/dags/ray-cluster.yaml",
        kuberay_version="1.0.0",
    )

    submit_job = SubmitRayJob(
        task_id="submit_ray_job",
        conn_id="ray_default",
        entrypoint="python script.py",
        runtime_env={"pip": ["requests"]},
        num_cpus=1,
        num_gpus=0,
        memory=1,
        wait_for_completion=True,
        job_timeout_seconds=600,
        poll_interval=30,
        fetch_logs=True,
        xcom_task_key="setup_ray_cluster.dashboard",
    )

    delete_cluster = DeleteRayCluster(
        task_id="delete_ray_cluster",
        conn_id="ray_default",
        ray_cluster_yaml="/opt/airflow/dags/ray-cluster.yaml",
    )

    setup_cluster >> submit_job >> delete_cluster
# [END howto_operator_ray_setup_teardown]
