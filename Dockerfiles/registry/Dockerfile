
FROM registry:2.8.3
LABEL ignis.build="optional"

# Install OpenSSL
RUN apk update && \
  apk add --no-cache openssl && \
  rm -rf "/var/cache/apk/*"
