
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

``apache-airflow-providers-ray``

Changelog
---------

1.0.0
.....

Initial version of the provider.

Features
~~~~~~~~

* ``Add Ray provider for distributed computing with Ray clusters on Kubernetes``
* ``Add SetupRayCluster, DeleteRayCluster, and SubmitRayJob operators``
* ``Add RayHook extending KubernetesHook for Ray cluster and job management``
* ``Add RayJobTrigger for deferred/async job monitoring``
* ``Add @task.ray decorator for TaskFlow API integration``

Based on the `astro-provider-ray <https://github.com/astronomer/astro-provider-ray>`__ package.
