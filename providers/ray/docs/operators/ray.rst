
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

.. _howto/operator:SetupRayCluster:

Ray Operators
=============

The Ray provider includes three operators for managing Ray clusters and jobs on Kubernetes.

SetupRayCluster
---------------

Use the :class:`~airflow.providers.ray.operators.ray.SetupRayCluster` to deploy a Ray cluster
on Kubernetes using the KubeRay operator. This operator installs the KubeRay Helm chart, creates
the Ray cluster from a YAML specification, optionally configures GPU device plugins, and waits
for the LoadBalancer to become ready.

Parameters
^^^^^^^^^^

conn_id
    The Airflow connection ID for the Ray/Kubernetes cluster.
ray_cluster_yaml
    Path to the YAML file defining the Ray cluster specification.
kuberay_version
    Version of the KubeRay operator Helm chart to install. Defaults to ``1.0.0``.
gpu_device_plugin_yaml
    URL or path to a GPU device plugin YAML (e.g., NVIDIA's ``k8s-device-plugin``). Optional.
update_if_exists
    Whether to update the cluster if it already exists. Defaults to ``False``.


.. _howto/operator:DeleteRayCluster:

DeleteRayCluster
----------------

Use the :class:`~airflow.providers.ray.operators.ray.DeleteRayCluster` to tear down a Ray cluster,
remove GPU device plugin DaemonSets, and uninstall the KubeRay operator.

Parameters
^^^^^^^^^^

conn_id
    The Airflow connection ID for the Ray/Kubernetes cluster.
ray_cluster_yaml
    Path to the YAML file defining the Ray cluster to delete.
gpu_device_plugin_yaml
    URL or path to the GPU device plugin YAML. Optional.


.. _howto/operator:SubmitRayJob:

SubmitRayJob
------------

Use the :class:`~airflow.providers.ray.operators.ray.SubmitRayJob` to submit a job to a Ray cluster
and optionally monitor it until completion. This operator supports:

- Automatic cluster setup and teardown (when ``ray_cluster_yaml`` is provided)
- Deferred (async) execution for long-running jobs
- Log streaming from the Ray job
- Dashboard URL retrieval via XCom

Parameters
^^^^^^^^^^

conn_id
    The Airflow connection ID for the Ray cluster.
entrypoint
    The command to execute as the Ray job (e.g., ``python script.py``).
runtime_env
    Runtime environment configuration dict (pip packages, working directory, etc.).
num_cpus
    Number of CPUs for the job entrypoint. Defaults to ``0``.
num_gpus
    Number of GPUs for the job entrypoint. Defaults to ``0``.
memory
    Memory in bytes for the job entrypoint. Defaults to ``0``.
resources
    Additional custom resources for the job. Optional.
ray_cluster_yaml
    Path to a Ray cluster YAML. If provided, the operator manages cluster lifecycle. Optional.
kuberay_version
    Version of KubeRay to use for cluster setup. Defaults to ``1.0.0``.
update_if_exists
    Whether to update an existing cluster. Defaults to ``True``.
gpu_device_plugin_yaml
    URL or path to the GPU device plugin YAML. Optional.
fetch_logs
    Whether to stream logs from the Ray job. Defaults to ``True``.
wait_for_completion
    Whether to wait for the job to finish. Defaults to ``True``. When enabled, the operator
    defers execution using a trigger for efficient resource usage.
job_timeout_seconds
    Maximum wait time in seconds. Defaults to ``600``. Set to ``0`` for no timeout.
poll_interval
    Seconds between status checks. Defaults to ``60``.
xcom_task_key
    XCom key to retrieve the dashboard URL (format: ``task_id.key``). Optional.


Usage Example
^^^^^^^^^^^^^

The following example shows a DAG that sets up a Ray cluster, submits a job, and tears
down the cluster:

.. exampleinclude:: /../../ray/tests/system/ray/example_ray.py
    :language: python
    :start-after: [START howto_operator_ray_setup_teardown]
    :end-before: [END howto_operator_ray_setup_teardown]
