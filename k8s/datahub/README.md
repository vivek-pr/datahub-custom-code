# DataHub Deployment Notes

This proof of concept assumes the official DataHub Helm charts are available in the `acryldata` Helm repository. The `make up` workflow expects that the DataHub core services are either already present in the target cluster or installed separately using those charts. The Action deployed in this repo can run alongside an existing DataHub deployment without additional configuration beyond providing the required secrets.
