from dagster import Definitions
from .defs.assets import hello_world, processed_data 

defs = Definitions(
    assets=[hello_world, processed_data],
    jobs=[],
)