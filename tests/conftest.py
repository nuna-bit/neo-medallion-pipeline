import os
import shutil

import pytest


@pytest.fixture(scope="session")
def spark():
    if not (os.environ.get("JAVA_HOME") or shutil.which("java")):
        pytest.skip("PySpark needs Java (JDK 17 or 21); set JAVA_HOME or add java to PATH")
    from pyspark.sql import SparkSession

    session = SparkSession.builder.master("local[1]").appName("neo-tests").getOrCreate()
    yield session
    session.stop()
