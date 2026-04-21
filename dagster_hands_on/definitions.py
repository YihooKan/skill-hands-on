from dagster import Definitions
from .defs.assets import hello_world

defs = Definitions(
    assets=[hello_world],
    jobs=[],
)