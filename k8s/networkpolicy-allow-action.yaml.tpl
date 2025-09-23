apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: allow-action-http-and-egress
  namespace: ${NAMESPACE}
  labels:
    app.kubernetes.io/name: tokenize-poc-action
spec:
  podSelector:
    matchLabels:
      app.kubernetes.io/name: tokenize-poc-action
  policyTypes:
    - Ingress
    - Egress
  ingress:
    - from:
        - podSelector: {}
      ports:
        - protocol: TCP
          port: 8080
  egress:
    - to:
        - namespaceSelector: {}
      ports:
        - protocol: UDP
          port: 53
        - protocol: TCP
          port: 53
    - to:
        - podSelector:
            matchLabels:
              app.kubernetes.io/name: postgresql
      ports:
        - protocol: TCP
          port: 5432
    - to:
        - ipBlock:
            cidr: 0.0.0.0/0
      ports:
        - protocol: TCP
          port: 80
        - protocol: TCP
          port: 443
