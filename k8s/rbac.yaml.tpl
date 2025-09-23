apiVersion: v1
kind: ServiceAccount
metadata:
  name: tokenize-poc-action
  namespace: ${NAMESPACE}
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: tokenize-poc-smoke
  namespace: ${NAMESPACE}
