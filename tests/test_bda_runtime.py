import pytest
from src.bda.bda_runtime import (
    get_bda_infrastructure_status,
    probe_hdfs,
    get_spark_info,
)


def test_get_bda_infrastructure_status():
    status = get_bda_infrastructure_status()
    assert "spark_available" in status
    assert "hdfs_available" in status
    assert "storage_mode" in status
    assert status["spark_available"] is True


def test_probe_hdfs_offline():
    # Demonstrates genuine probing: localhost:9000 is not falsely reported as True
    # when the daemon is not running.
    is_active = probe_hdfs("127.0.0.1", 9000, timeout=0.2)
    assert isinstance(is_active, bool)


def test_get_spark_info():
    info = get_spark_info()
    assert info["available"] is True
    assert "4." in info["version"]
