# Angular dashboard: built once, configured at start.
#
#   docker build -f dashboard/Dockerfile -t dashboard dashboard

FROM node:22-alpine AS build
WORKDIR /build

# Dependencies as their own layer: the manifest changes far less often than the
# sources, so editing a component does not reinstall the world.
COPY package.json package-lock.json ./
# --legacy-peer-deps because npm 10.9's peer resolver crashes on the vitest
# dependency graph with "Cannot read properties of null (reading 'edgesOut')".
# The lock file was produced the same way, so the install is still reproducible.
RUN npm ci --no-audit --no-fund --legacy-peer-deps

COPY . .
RUN npm run build -- --configuration production

FROM nginx:alpine
# Unprivileged: it serves static files and proxies a port.
RUN addgroup -S dashboard && adduser -S dashboard -G dashboard

COPY --from=build /build/dist/dashboard/browser /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY docker-entrypoint.sh /docker-entrypoint.d/99-runtime-config.sh
RUN chmod +x /docker-entrypoint.d/99-runtime-config.sh

EXPOSE 8080

HEALTHCHECK --interval=15s --timeout=3s --start-period=5s --retries=3 \
  CMD wget -q -O - http://127.0.0.1:8080/healthz || exit 1
