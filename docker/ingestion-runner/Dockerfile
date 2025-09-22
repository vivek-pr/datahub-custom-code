# syntax=docker/dockerfile:1.7
FROM acryldata/datahub-ingestion:v1.2.0.1

# put it in a standard bin dir and set perms at copy
COPY --chmod=0755 entrypoint.sh /usr/local/bin/ingestion-entrypoint.sh

ENTRYPOINT ["/usr/local/bin/ingestion-entrypoint.sh"]
