apiVersion: v1
kind: Service
metadata:
  name: tokenize-poc-action
  namespace: ${NAMESPACE}
  labels:
    app: tokenize-poc-action
spec:
  selector:
    app: tokenize-poc-action
  ports:
    - port: 8080
      targetPort: 8080
      name: http
