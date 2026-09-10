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

# Everything nginx writes to, handed to the user that will run it. Each path is
# here because dropping root without it makes the container fail to start:
#
#   /var/cache/nginx      proxy and temp buffers, opened at startup
#   /var/run/nginx.pid    the master writes its pid before forking a worker
#   .../html/assets       the runtime config the entrypoint rewrites on boot
#
# The listener is already on 8080, so no capability to bind a privileged port
# is needed -- which is what makes dropping root possible at all here.
RUN mkdir -p /var/cache/nginx /usr/share/nginx/html/assets \
 && touch /var/run/nginx.pid \
 && chown -R dashboard:dashboard /var/cache/nginx /var/run/nginx.pid /usr/share/nginx/html

USER dashboard

EXPOSE 8080

HEALTHCHECK --interval=15s --timeout=3s --start-period=5s --retries=3 \
  CMD wget -q -O - http://127.0.0.1:8080/healthz || exit 1
