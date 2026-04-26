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
"""Example DAG demonstrating the @task.ray decorator."""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task

# [START howto_decorator_ray]
RAY_TASK_CONFIG = {
    "conn_id": "ray_default",
    "runtime_env": {"pip": ["numpy"]},
    "num_cpus": 1,
    "num_gpus": 0,
    "memory": 1,
    "ray_cluster_yaml": "/opt/airflow/dags/ray-cluster.yaml",
    "poll_interval": 5,
    "job_timeout_seconds": 600,
}


@dag(
    dag_id="example_ray_decorator",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "ray"],
)
def ray_taskflow_example():
    @task
    def generate_data():
        return [1, 2, 3, 4, 5]

    @task.ray(config=RAY_TASK_CONFIG)
    def process_data_with_ray(data):
        import numpy as np

        return float(np.mean([x**2 for x in data]))

    data = generate_data()
    process_data_with_ray(data)


ray_taskflow_example()
# [END howto_decorator_ray]
