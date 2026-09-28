import os

import pytest

from metaflow_extensions.spark.plugins.backends import local
from metaflow_extensions.spark.plugins.backends.local import _ensure_java_home


def _make_java(root):
    bin_dir = root / "bin"
    bin_dir.mkdir(parents=True)
    (bin_dir / "java").write_text("")


@pytest.fixture
def no_java(monkeypatch):
    monkeypatch.delenv("JAVA_HOME", raising=False)
    monkeypatch.setattr(local.shutil, "which", lambda name: None)


@pytest.mark.parametrize("layout", [("lib", "jvm"), ()])
def test_java_home_found_in_environment_prefix(tmp_path, no_java, layout):
    home = tmp_path.joinpath(*layout)
    _make_java(home)
    _ensure_java_home(str(tmp_path))
    assert os.environ["JAVA_HOME"] == str(home)


def test_java_home_prefers_conda_jvm_dir(tmp_path, no_java):
    _make_java(tmp_path / "lib" / "jvm")
    _make_java(tmp_path)
    _ensure_java_home(str(tmp_path))
    assert os.environ["JAVA_HOME"] == str(tmp_path / "lib" / "jvm")


def test_existing_java_home_is_kept(tmp_path, monkeypatch):
    monkeypatch.setenv("JAVA_HOME", "/opt/jdk")
    _make_java(tmp_path / "lib" / "jvm")
    _ensure_java_home(str(tmp_path))
    assert os.environ["JAVA_HOME"] == "/opt/jdk"


def test_java_on_path_is_kept(tmp_path, monkeypatch):
    monkeypatch.delenv("JAVA_HOME", raising=False)
    monkeypatch.setattr(local.shutil, "which", lambda name: "/usr/bin/java")
    _make_java(tmp_path / "lib" / "jvm")
    _ensure_java_home(str(tmp_path))
    assert "JAVA_HOME" not in os.environ


def test_no_jdk_leaves_java_home_unset(tmp_path, no_java):
    _ensure_java_home(str(tmp_path))
    assert "JAVA_HOME" not in os.environ
