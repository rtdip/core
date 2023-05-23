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

import sys
from rtdip.tasks.common import Task
from rtdip_sdk.pipelines.execute import * # NOSONAR
from rtdip_sdk.pipelines.sources import * # NOSONAR
from rtdip_sdk.pipelines.transformers import * # NOSONAR
from rtdip_sdk.pipelines.destinations import * # NOSONAR
from rtdip_sdk.pipelines.utilities import * # NOSONAR
from rtdip_sdk.pipelines.converters import * # NOSONAR

class RTDIPPipelineTask(Task):
    def launch(self):
        self.logger.info("Launching RTDIP Pipeline Task")
        self.logger.info("Job to execute {}".format(sys.argv[1]))
        pipeline_job = PipelineJobFromJsonConverter(sys.argv[1]).convert()
        pipeline_job_execute = PipelineJobExecute(pipeline_job)
        pipeline_job_execute.run()
        self.logger.info("RTDIP Pipeline Task completed")
        
# if you're using python_wheel_task, you'll need the entrypoint function to be used in setup.py
def entrypoint():  # pragma: no cover
    task = RTDIPPipelineTask()
    task.launch()

# if you're using spark_python_task, you'll need the __main__ block to start the code execution
if __name__ == '__main__':
    entrypoint()        