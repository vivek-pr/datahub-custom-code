apiVersion: apps/v1
kind: Deployment
metadata:
  name: tokenize-poc-action
  namespace: ${NAMESPACE}
  labels:
    app.kubernetes.io/name: tokenize-poc-action
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: tokenize-poc-action
  template:
    metadata:
      labels:
        app.kubernetes.io/name: tokenize-poc-action
    spec:
      serviceAccountName: tokenize-poc-action
      securityContext:
        runAsUser: 10001
        runAsNonRoot: true
      containers:
        - name: action
          image: ${IMAGE_REF}
          imagePullPolicy: IfNotPresent
          ports:
            - containerPort: 8080
              name: http
          env:
            - name: PG_CONN_STR
              valueFrom:
                secretKeyRef:
                  name: tokenize-poc-secrets
                  key: PG_CONN_STR
            - name: DBX_JDBC_URL
              valueFrom:
                secretKeyRef:
                  name: tokenize-poc-secrets
                  key: DBX_JDBC_URL
            - name: TOKEN_SDK_MODE
              valueFrom:
                secretKeyRef:
                  name: tokenize-poc-secrets
                  key: TOKEN_SDK_MODE
          readinessProbe:
            httpGet:
              path: /healthz
              port: http
            initialDelaySeconds: 2
            periodSeconds: 3
            failureThreshold: 10
          livenessProbe:
            httpGet:
              path: /healthz
              port: http
            initialDelaySeconds: 5
            periodSeconds: 10
          resources:
            requests:
              cpu: 50m
              memory: 64Mi
            limits:
              cpu: 200m
              memory: 256Mi
