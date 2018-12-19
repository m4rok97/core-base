
FROM ignisframework/base-full

RUN apt -y install rapidjson-dev
COPY --from=ignisframework/cpp-executor ${IGNIS_HOME} ${IGNIS_HOME}
RUN cp -R ${IGNIS_HOME}/core/cpp/* /


