/usr/local/bin/docker-entrypoint.sh generate \
    -g typescript-fetch \
    -i /project/pkg/api/api.swagger.json \
    -o /project/internal/lookout/ui/src/openapi/armada
/usr/local/bin/docker-entrypoint.sh generate \
    -g typescript-fetch \
    -i /project/pkg/api/binoculars/api.swagger.json \
    -o /project/internal/lookout/ui/src/openapi/binoculars
/usr/local/bin/docker-entrypoint.sh generate \
    -g typescript-fetch \
    -i /project/pkg/api/schedulerobjects/api.swagger.json \
    -o /project/internal/lookout/ui/src/openapi/schedulerobjects
