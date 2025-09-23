apiVersion: v1
kind: Service
metadata:
  name: tokenize-poc-action
  namespace: ${NAMESPACE}
  labels:
    app.kubernetes.io/name: tokenize-poc-action
spec:
  selector:
    app.kubernetes.io/name: tokenize-poc-action
  ports:
    - name: http
      port: 8080
      targetPort: 8080
