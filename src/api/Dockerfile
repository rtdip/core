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

FROM mcr.microsoft.com/azure-functions/python:4-python3.10

ENV AzureWebJobsScriptRoot=/home/site/wwwroot \
    AzureFunctionsJobHost__Logging__Console__IsEnabled=true

RUN rm -rf /var/lib/apt/lists/partial \
    apt-get clean \
    && apt-get update -o Acquire::CompressionTypes::Order::=gz \
    && apt-get install -y odbcinst1debian2 libodbc1 odbcinst unixodbc unixodbc-dev libsasl2-dev libsasl2-modules-gssapi-mit libboost-all-dev \
    && apt-get install -y ca-certificates curl python3-pip python3-dev python3-setuptools python3-wheel \
    && apt-get install -y zip wget \ 
    && wget https://databricks-bi-artifacts.s3.us-east-2.amazonaws.com/simbaspark-drivers/odbc/2.6.29/SimbaSparkODBC-2.6.29.1049-Debian-64bit.zip -P /odbc/ \
    && unzip /odbc/SimbaSparkODBC-2.6.29.1049-Debian-64bit.zip -d /odbc \
    && dpkg -i /odbc/simbaspark_2.6.29.1049-2_amd64.deb \
    && rm /odbc -r

RUN echo '[ODBC Drivers]' > /etc/odbcinst.ini \
    && echo 'Simba Spark ODBC Driver = Installed' >> /etc/odbcinst.ini \
    && echo '[Simba Spark ODBC Driver]' >> /etc/odbcinst.ini \
    && echo 'Driver = /opt/simba/spark/lib/64/libsparkodbc_sb64.so' >> /etc/odbcinst.ini

COPY src/api/requirements.txt /
RUN pip install --no-cache-dir pyarrow==12.0.0 \
    && python -c "import pyarrow; pyarrow.create_library_symlinks()" \
    && CFLAGS="-D_GLIBCXX_USE_CXX11_ABI=0" pip install --no-cache-dir -r /requirements.txt

COPY src/api/ /home/site/wwwroot
COPY src /home/site/wwwroot/src
