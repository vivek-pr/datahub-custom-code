apiVersion: v1
kind: Secret
metadata:
  name: tokenize-poc-secrets
  namespace: ${NAMESPACE}
type: Opaque
data:
  PG_CONN_STR: ${PG_CONN_STR_B64}
  DBX_JDBC_URL: ${DBX_JDBC_URL_B64}
  TOKEN_SDK_MODE: ${TOKEN_SDK_MODE_B64}
