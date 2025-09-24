
# superset/Dockerfile
FROM apache/superset:3.1.0

USER root

# Copiamos el script de arranque
COPY docker-init.sh /app/docker-init.sh

# Convertimos CRLF -> LF y damos permisos de ejecución
RUN sed -i 's/\r$//' /app/docker-init.sh && chmod +x /app/docker-init.sh

# Forzamos ejecutar con bash para evitar "exec format error"
ENTRYPOINT ["bash", "/app/docker-init.sh"]

USER superset