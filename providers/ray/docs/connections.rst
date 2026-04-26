
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

.. _howto/connection:ray:

Ray Connection
==============

The `Ray <https://docs.ray.io/>`__ connection type enables the Ray provider to connect to
Ray clusters deployed on Kubernetes via the KubeRay operator.

Default Connection IDs
----------------------

The Ray hook uses ``ray_default`` as the default connection ID.

Configuring the Connection
--------------------------

Ray Dashboard URL (optional)
    The URL of the Ray dashboard (e.g., ``http://ray-dashboard:8265``).
    Can also be set via the ``RAY_ADDRESS`` environment variable.

Cookies (optional)
    Authentication cookies for the Ray dashboard, if required.

Metadata (optional)
    Additional metadata to send with Ray job requests.

Headers (optional)
    HTTP headers as a JSON string (e.g., ``{"Authorization": "Bearer token"}``).

Verify (optional)
    Whether to verify SSL certificates when connecting to the Ray dashboard.

Kube Config Path (optional)
    Path to a kubeconfig file for Kubernetes cluster access.
    Mutually exclusive with ``Kube Config (JSON format)``.

Kube Config (JSON format) (optional)
    Kubeconfig content as a JSON string.
    Mutually exclusive with ``Kube Config Path``.

Namespace (optional)
    The Kubernetes namespace where the Ray cluster is deployed. Defaults to ``default``.

Cluster Context (optional)
    The Kubernetes cluster context to use from the kubeconfig.

Disable SSL (optional)
    Disable SSL verification for Kubernetes API calls.

Disable TCP Keepalive (optional)
    Disable TCP keepalive for Kubernetes API connections.

.. note::

    You must provide either ``Kube Config Path`` or ``Kube Config (JSON format)``, but not both.
    If neither is provided, the hook will attempt to use in-cluster Kubernetes configuration.
