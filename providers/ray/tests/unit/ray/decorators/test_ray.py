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
from __future__ import annotations

from datetime import timedelta
from unittest.mock import MagicMock, patch

import pytest

from airflow.providers.ray.decorators.ray import _RayDecoratedOperator, ray_task
from airflow.providers.ray.exceptions import RayJobException


class TestRayDecoratedOperator:
    @patch("airflow.providers.ray.decorators.ray.SubmitRayJob.__init__", return_value=None)
    @patch("airflow.providers.ray.decorators.ray.DecoratedOperator.__init__", return_value=None)
    def test_init(self, mock_decorated_init, mock_submit_init):
        config = {
            "conn_id": "test_conn",
            "entrypoint": "python script.py",
            "runtime_env": {"pip": ["package1"]},
            "num_cpus": 2,
            "num_gpus": 1,
            "memory": 1000,
            "ray_cluster_yaml": "cluster.yaml",
            "kuberay_version": "1.1.0",
            "gpu_device_plugin_yaml": "gpu.yaml",
            "fetch_logs": False,
            "wait_for_completion": False,
            "job_timeout_seconds": 1200,
            "poll_interval": 30,
            "xcom_task_key": "task.key",
        }
        operator = _RayDecoratedOperator(config=config, task_id="test_task")
        assert operator._config == config

    @patch("airflow.providers.ray.decorators.ray.SubmitRayJob.__init__", return_value=None)
    @patch("airflow.providers.ray.decorators.ray.DecoratedOperator.__init__", return_value=None)
    def test_load_config_defaults(self, mock_decorated_init, mock_submit_init):
        operator = _RayDecoratedOperator(config={}, task_id="test_task")
        operator._load_config({})
        assert operator.conn_id == ""
        assert operator.entrypoint == "python script.py"
        assert operator.runtime_env == {}
        assert operator.num_cpus == 1
        assert operator.num_gpus == 0
        assert operator.memory == 1
        assert operator.job_timeout_seconds == timedelta(seconds=600)

    @patch("airflow.providers.ray.decorators.ray.SubmitRayJob.__init__", return_value=None)
    @patch("airflow.providers.ray.decorators.ray.DecoratedOperator.__init__", return_value=None)
    def test_load_config_with_callable(self, mock_decorated_init, mock_submit_init):
        def config_callable(context=None):
            return {
                "conn_id": "dynamic_conn",
                "num_cpus": 4,
            }

        operator = _RayDecoratedOperator(config=config_callable, task_id="test_task")
        context = MagicMock()
        config = operator._build_config(context)
        assert config == {"conn_id": "dynamic_conn", "num_cpus": 4}

    @patch("airflow.providers.ray.decorators.ray.SubmitRayJob.__init__", return_value=None)
    @patch("airflow.providers.ray.decorators.ray.DecoratedOperator.__init__", return_value=None)
    def test_load_config_invalid_num_cpus(self, mock_decorated_init, mock_submit_init):
        operator = _RayDecoratedOperator(config={}, task_id="test_task")
        with pytest.raises(RayJobException, match="num_cpus should be an integer or float value"):
            operator._load_config({"num_cpus": "invalid_number"})

    @patch("airflow.providers.ray.decorators.ray.SubmitRayJob.__init__", return_value=None)
    @patch("airflow.providers.ray.decorators.ray.DecoratedOperator.__init__", return_value=None)
    def test_load_config_invalid_num_gpus(self, mock_decorated_init, mock_submit_init):
        operator = _RayDecoratedOperator(config={}, task_id="test_task")
        with pytest.raises(RayJobException, match="num_gpus should be an integer or float value"):
            operator._load_config({"num_gpus": "invalid_number"})

    @patch("airflow.providers.ray.decorators.ray.SubmitRayJob.__init__", return_value=None)
    @patch("airflow.providers.ray.decorators.ray.DecoratedOperator.__init__", return_value=None)
    def test_extract_function_body(self, mock_decorated_init, mock_submit_init):
        operator = _RayDecoratedOperator(config={}, task_id="test_task")

        source = """@task.ray(config={"conn_id": "test"})
def my_function():
    return "hello"
"""
        with patch.object(type(operator), "log", new_callable=lambda: property(lambda self: MagicMock())):
            result = operator._extract_function_body(source)
        assert "def my_function():" in result
        assert "@task.ray" not in result

    @patch("airflow.providers.ray.decorators.ray.SubmitRayJob.__init__", return_value=None)
    @patch("airflow.providers.ray.decorators.ray.DecoratedOperator.__init__", return_value=None)
    def test_extract_function_body_no_decorator(self, mock_decorated_init, mock_submit_init):
        operator = _RayDecoratedOperator(config={}, task_id="test_task")

        source = """def my_function():
    return "hello"
"""
        with patch.object(type(operator), "log", new_callable=lambda: property(lambda self: MagicMock())):
            with pytest.raises(RayJobException, match="Expects the `@task.ray` or `@ray.task` decorator"):
                operator._extract_function_body(source)


class TestRayTaskDecorator:
    def test_ray_task_decorator(self):
        @ray_task(config={"conn_id": "test"})
        def dummy_function():
            return "hello"

        assert dummy_function.operator_class == _RayDecoratedOperator

    def test_ray_task_multiple_outputs(self):
        @ray_task(multiple_outputs=True, config={"conn_id": "test"})
        def dummy_function():
            return {"key": "value"}

        assert dummy_function.operator_class == _RayDecoratedOperator
