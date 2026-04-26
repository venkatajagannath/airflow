
 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

.. _howto/decorator:ray:

Task Ray Decorator
==================

The ``@task.ray`` decorator wraps a Python callable and submits it as a Ray job to a remote
Ray cluster. The decorated function's source code is extracted, written to a temporary script,
and executed on the Ray cluster via the :class:`~airflow.providers.ray.operators.ray.SubmitRayJob`
operator.

This allows you to use the Airflow TaskFlow API to define distributed Ray workloads inline
in your DAG files.

Parameters
----------

The ``@task.ray`` decorator accepts a ``config`` parameter — either a dictionary or a callable
that returns a dictionary — with the following keys:

conn_id
    The Airflow connection ID for the Ray cluster.
runtime_env
    Runtime environment dict for the Ray job (e.g., ``{"pip": ["numpy", "pandas"]}``).
num_cpus
    Number of CPUs for the job. Defaults to ``1``.
num_gpus
    Number of GPUs for the job. Defaults to ``0``.
memory
    Memory for the job in bytes. Defaults to ``1``.
resources
    Additional custom resources. Optional.
ray_cluster_yaml
    Path to a Ray cluster YAML for automatic cluster lifecycle. Optional.
kuberay_version
    KubeRay Helm chart version. Defaults to ``1.0.0``.
gpu_device_plugin_yaml
    Path or URL to a GPU device plugin YAML. Optional.
fetch_logs
    Whether to stream logs. Defaults to ``True``.
wait_for_completion
    Whether to wait for job completion. Defaults to ``True``.
job_timeout_seconds
    Maximum wait time in seconds. Defaults to ``600``.
poll_interval
    Seconds between status checks. Defaults to ``60``.
xcom_task_key
    XCom key for dashboard URL retrieval. Optional.

You can also pass ``multiple_outputs=True`` to unroll the return value into multiple XCom values.

Dynamic Configuration
---------------------

The ``config`` parameter can be a callable that receives keyword arguments and optionally an
Airflow ``context``. This enables runtime configuration based on DAG run parameters:

.. code-block:: python

    def get_config(context=None):
        cluster_size = context["params"].get("cluster_size", "small")
        return {
            "conn_id": "ray_default",
            "num_cpus": 4 if cluster_size == "large" else 1,
            "runtime_env": {"pip": ["numpy"]},
        }

    @task.ray(config=get_config)
    def my_ray_task(data):
        ...

Usage Example
-------------

.. exampleinclude:: /../../ray/tests/system/ray/example_ray_decorator.py
    :language: python
    :start-after: [START howto_decorator_ray]
    :end-before: [END howto_decorator_ray]
