
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
===================================


.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Basics

    Home <self>
    Changelog <changelog>
    Security <security>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Guides

    Connection types <connections>
    Operators <operators/ray>
    Task Ray Decorator <decorators/ray>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Resources

    Python API <_api/airflow/providers/ray/index>
    PyPI Repository <https://pypi.org/project/apache-airflow-providers-ray/>
    Installing from sources <installing-providers-from-sources>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: System tests

    System Tests <_api/tests/system/ray/index>

.. THE REMAINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


apache-airflow-providers-ray package
------------------------------------------------------

`Ray <https://docs.ray.io/>`__ provider for Apache Airflow.
Provides operators, hooks, and a task decorator for submitting and managing
Ray jobs on Kubernetes clusters using the KubeRay operator.

Benefits of using this provider include:

- **Integration**: Incorporate Ray jobs into Airflow DAGs for unified workflow management.
- **Distributed computing**: Use Ray's distributed capabilities within Airflow pipelines for scalable ETL, LLM fine-tuning, and more.
- **Monitoring**: Track Ray job progress through Airflow's UI and log streaming.
- **Cluster lifecycle**: Automatically set up and tear down Ray clusters as part of your DAG.


Release: 1.0.0

Provider package
----------------

This package is for the ``ray`` provider.
All classes for this package are included in the ``airflow.providers.ray`` python package.

Installation
------------

You can install this package on top of an existing Airflow installation via
``pip install apache-airflow-providers-ray``.
For the minimum Airflow version supported, see ``Requirements`` below.

Requirements
------------

The minimum Apache Airflow version supported by this provider distribution is ``2.11.0``.

==================================================  ==================
PIP package                                         Version required
==================================================  ==================
``apache-airflow``                                  ``>=2.11.0``
``apache-airflow-providers-common-compat``          ``>=1.12.0``
``apache-airflow-providers-cncf-kubernetes``        ``>=10.0.0``
``PyYAML``                                          ``>=6.0``
``ray[default]``                                    ``>=2.0.0``
``requests``                                        ``>=2.27.0``
==================================================  ==================

Cross provider package dependencies
------------------------------------

Those are dependencies that might be needed in order to use all the features of the package.
You need to install the specified provider distributions in order to use them.

You can install such cross-provider dependencies when installing from PyPI. For example:

.. code-block:: bash

    pip install apache-airflow-providers-ray

======================================================================================================================  =====================
Dependent package                                                                                                       Extra
======================================================================================================================  =====================
`apache-airflow-providers-cncf-kubernetes <https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes>`_  ``cncf.kubernetes``
======================================================================================================================  =====================
