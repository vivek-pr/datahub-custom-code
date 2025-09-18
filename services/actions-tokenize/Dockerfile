ARG DATAHUB_ACTIONS_IMAGE=acryldata/datahub-actions:head
FROM ${DATAHUB_ACTIONS_IMAGE}

USER root
WORKDIR /app

COPY services/actions-tokenize/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY services/actions-tokenize/actions_tokenize /opt/actions-tokenize/actions_tokenize
COPY services/actions-tokenize/config /opt/actions-tokenize/config

ENV PYTHONPATH=/opt/actions-tokenize:${PYTHONPATH}
ENV DATAHUB_ACTIONS_CONFIG=/opt/actions-tokenize/config/actions-tokenize.yaml

USER ${ACTIONS_USER:-datahub}
