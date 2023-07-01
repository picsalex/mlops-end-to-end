FROM python:3.11.4-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Specify app location
ARG HOME_DIRECTORY=/usr/local/src
WORKDIR $HOME_DIRECTORY

ENV POETRY_HOME=${HOME_DIRECTORY}/poetry \
    VIRTUAL_ENV=${HOME_DIRECTORY}/venv
ENV PATH="$VIRTUAL_ENV/bin:$POETRY_HOME/bin:$PATH"

RUN apt-get update && apt-get upgrade -y
RUN pip install --no-cache -U pip

RUN python -m venv ${POETRY_HOME} ${VIRTUAL_ENV} && \
    ${POETRY_HOME}/bin/pip install --no-cache-dir 'poetry>=1.5.0'

# Install dependencies
COPY ./pyproject.toml ./poetry.lock ${HOME_DIRECTORY}/
RUN poetry config installer.modern-installation false && \
    poetry install --no-cache && \
    rm -fr /root/.cache
