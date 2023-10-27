# Copyright 2022 RTDIP
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import sys
import inspect
from typing import List, Tuple

from .interfaces import UtilitiesInterface
from .._pipeline_utils.models import Libraries, SystemType


class PipelineComponentsGetUtility(UtilitiesInterface):
    """
    Gets the list of imported RTDIP components. Returns the libraries and settings of the components to be used in the pipeline.

    Call this component after all imports of the RTDIP components to ensure that the components can be determined.

    Parameters:
        module (optional str): Provide the module to use for imports of rtdip-sdk components. If not populated, it will use the calling module to check for imports
        spark_config (optional dict): Additional spark configuration to be applied to the spark session
    """

    def __init__(self, module: str = None, spark_config: dict = None) -> None:
        if module == None:
            frm = inspect.stack()[1]
            mod = inspect.getmodule(frm[0])
            self.module = mod.__name__
        else:
            self.module = module
        self.spark_config = {} if spark_config is None else spark_config

    @staticmethod
    def system_type():
        """
        Attributes:
            SystemType (Environment): Requires PYTHON
        """
        return SystemType.PYTHON

    @staticmethod
    def libraries():
        libraries = Libraries()
        return libraries

    @staticmethod
    def settings() -> dict:
        return {}

    def execute(self) -> Tuple[Libraries, dict]:
        from ..sources.interfaces import SourceInterface
        from ..destinations.interfaces import DestinationInterface
        from ..deploy.interfaces import DeployInterface
        from ..secrets.interfaces import SecretsInterface
        from ..transformers.interfaces import TransformerInterface

        try:
            classes_imported = inspect.getmembers(
                sys.modules[self.module], inspect.isclass
            )
            component_list = []
            for cls in classes_imported:
                class_check = getattr(sys.modules[self.module], cls[0])
                if (
                    (
                        issubclass(class_check, SourceInterface)
                        and class_check != SourceInterface
                    )
                    or (
                        issubclass(class_check, DestinationInterface)
                        and class_check != DestinationInterface
                    )
                    or (
                        issubclass(class_check, DeployInterface)
                        and class_check != DeployInterface
                    )
                    or (
                        issubclass(class_check, SecretsInterface)
                        and class_check != SecretsInterface
                    )
                    or (
                        issubclass(class_check, TransformerInterface)
                        and class_check != TransformerInterface
                    )
                    or (
                        issubclass(class_check, UtilitiesInterface)
                        and class_check != UtilitiesInterface
                    )
                ):
                    component_list.append(cls[1])

            task_libraries = Libraries()
            task_libraries.get_libraries_from_components(component_list)
            spark_configuration = self.spark_config
            for component in component_list:
                spark_configuration = {**spark_configuration, **component.settings()}
            return (task_libraries, spark_configuration)

        except Exception as e:
            logging.exception(str(e))
            raise e
