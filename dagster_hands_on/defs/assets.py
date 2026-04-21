import dagster as dg

@dg.asset
def hello_world(context: dg.AssetExecutionContext) -> str:
    """A simple asset that returns a greeting message"""
    context.log.info("Executing hello_world asset")
    return "Hello, Dagster!"
