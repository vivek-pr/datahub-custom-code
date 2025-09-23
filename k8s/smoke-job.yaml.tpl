apiVersion: batch/v1
kind: Job
metadata:
  name: tokenize-poc-smoke
  namespace: ${NAMESPACE}
spec:
  template:
    metadata:
      labels:
        app: tokenize-poc-smoke
    spec:
      serviceAccountName: tokenize-poc-smoke
      restartPolicy: Never
      containers:
        - name: runner
          image: python:3.11-slim
          env:
            - name: DBX_JDBC_URL
              valueFrom:
                secretKeyRef:
                  name: tokenize-poc-secrets
                  key: DBX_JDBC_URL
          command:
            - /bin/sh
          args:
            - -c
            - |
              set -euo pipefail
              apt-get update >/dev/null
              apt-get install -y curl >/dev/null
              SERVICE="http://tokenize-poc-action:8080/trigger"
              PG_DATASET='urn:li:dataset:(urn:li:dataPlatform:postgres,postgres.schema.customers,PROD)'
              DBX_DATASET='urn:li:dataset:(urn:li:dataPlatform:databricks,tokenize.schema.customers,PROD)'
              COLUMNS='["email","phone"]'
              PAYLOAD() {
                printf '{"dataset":%s,"columns":%s,"limit":100}' "$1" "$2"
              }
              run_twice() {
                local name=$1 dataset=$2
                echo "Triggering $name tokenization"
                local first second updated
                first=$(curl -sS --fail-with-body -X POST -H 'Content-Type: application/json' -d "$(PAYLOAD "$dataset" "$COLUMNS")" "$SERVICE")
                echo "$first"
                updated=$(python -c 'import json,sys;print(json.loads(sys.stdin.read()).get("updated_count",-1))' <<<"$first")
                if [ "$updated" -le 0 ]; then
                  echo "Expected $name first run to update rows, saw $updated" >&2
                  return 1
                fi
                second=$(curl -sS --fail-with-body -X POST -H 'Content-Type: application/json' -d "$(PAYLOAD "$dataset" "$COLUMNS")" "$SERVICE")
                echo "$second"
                updated=$(python -c 'import json,sys;print(json.loads(sys.stdin.read()).get("updated_count",-1))' <<<"$second")
                if [ "$updated" -ne 0 ]; then
                  echo "Expected $name second run to be idempotent, saw $updated" >&2
                  return 1
                fi
              }
              run_twice "Postgres" "$PG_DATASET"
              if [ -n "$DBX_JDBC_URL" ]; then
                run_twice "Databricks" "$DBX_DATASET"
              else
                echo "DBX_JDBC_URL not configured; skipping Databricks smoke"
              fi
