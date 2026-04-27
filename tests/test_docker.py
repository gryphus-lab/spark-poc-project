import re
import shutil
import subprocess
from pathlib import Path

import pytest

DOCKERFILE = Path(__file__).resolve().parent.parent / "Dockerfile"
DOCKER_COMPOSE = Path(__file__).resolve().parent.parent / "docker-compose.yml"
EXPECTED_SERVICES = [
    "spark-master",
    "spark-worker",
    "spark-submit",
    "minio",
    "minio-setup",
    "catalog",
]


def test_dockerfile_exists_and_contains_required_stages():
    assert DOCKERFILE.exists(), "Dockerfile must exist in the repository root"
    content = DOCKERFILE.read_text()

    assert "FROM apache/spark:3.5.0 AS builder" in content
    assert "FROM apache/spark:3.5.0" in content
    assert "COPY requirements-runtime.txt ." in content
    assert "COPY ./src src" in content
    assert "COPY ./apps apps" in content
    assert "USER 185" in content
    assert 'ENV PYTHONPATH="/opt/spark/python:/opt/spark/project/src"' in content
    assert "pip install --no-cache-dir -r requirements-runtime.txt" in content


def test_docker_compose_file_has_expected_services():
    assert DOCKER_COMPOSE.exists(), (
        "docker-compose.yml must exist in the repository root"
    )
    content = DOCKER_COMPOSE.read_text()

    assert "services:" in content
    assert "volumes:" in content
    assert "minio-data:" in content

    for service in EXPECTED_SERVICES:
        assert re.search(rf"^\s*{service}:", content, re.MULTILINE), (
            f"Expected service '{service}' in docker-compose.yml"
        )

    assert "spark://spark-master:7077" in content
    assert "minio:9000" in content
    assert "condition: service_healthy" in content


def _docker_compose_command():
    docker = shutil.which("docker")
    docker_compose = shutil.which("docker-compose")

    if docker is None and docker_compose is None:
        return None

    # Probe for docker compose v2 plugin
    if docker is not None:
        try:
            result = subprocess.run(
                [docker, "compose", "version"],
                capture_output=True,
                timeout=5,
                check=False,
            )
            if result.returncode == 0:
                return [docker, "compose", "-f", str(DOCKER_COMPOSE), "config"]
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pass

    # Fall back to docker-compose binary if available
    if docker_compose is not None:
        return [docker_compose, "-f", str(DOCKER_COMPOSE), "config"]

    return None


@pytest.mark.skipif(
    _docker_compose_command() is None,
    reason="Docker CLI or docker-compose is not installed; skipping compose validation",
)
def test_docker_compose_config_validates():
    command = _docker_compose_command()
    assert command is not None

    completed = subprocess.run(
        command,
        capture_output=True,
        text=True,
        cwd=Path(__file__).resolve().parent.parent,
        check=False,
    )

    if completed.returncode != 0:
        raise AssertionError(
            f"docker compose validation failed:\nstdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
        )

    for service in EXPECTED_SERVICES:
        assert re.search(rf"^\s*{service}:", completed.stdout, re.MULTILINE), (
            f"Expected service '{service}' in docker compose config output"
        )
