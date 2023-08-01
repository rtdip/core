from src.sdk.python.rtdip_sdk.pipelines.sources.python.delta import PythonDeltaSource
from src.sdk.python.rtdip_sdk.pipelines.destinations.python.delta import PythonDeltaDestination
from src.sdk.python.rtdip_sdk.pipelines.transformers.python.test_delta_transformer import DeltaTransformer
from polars.polars import PyLazyFrame

source= PythonDeltaSource("/Users/James.Broady/Documents/test_table").read_stream()
transform = DeltaTransformer(source).transform()

destination = PythonDeltaDestination(transform,"/Users/James.Broady/Documents/test_table_copy")
destination.write_stream()

# PyLazyFrame.new_from_csv()
# PyLazyFrame.optimization_toggle()