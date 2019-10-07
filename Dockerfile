FROM python:3.6

ENV PIPENV_VERSION_TO_INSTALL="2018.11.26"
ENV PIP_VERSION_TO_INSTALL="18.1"

RUN pip install -U pip==${PIP_VERSION_TO_INSTALL} && \
    pip install -U pipenv==${PIPENV_VERSION_TO_INSTALL}

ARG SIT_PYPI_USER_VAR
ARG SIT_PYPI_PASS_VAR
ENV SIT_PYPI_USER $SIT_PYPI_USER_VAR
ENV SIT_PYPI_PASS $SIT_PYPI_PASS_VAR

ADD ./Pipfile /service/Pipfile
ADD ./setup.py /service/setup.py
RUN mkdir -p /service/benchmark_tools/ && \
    touch /service/benchmark_tools/__init__.py
WORKDIR /service
RUN rm -f Pipfile.lock && pipenv lock -vvv && pipenv --rm && \
    pipenv install --system  && \
    rm -rf /tmp/pip* /root/.cache

ADD . /service
RUN pip install -e . && \
    rm -rf /tmp/pip* /root/.cache


