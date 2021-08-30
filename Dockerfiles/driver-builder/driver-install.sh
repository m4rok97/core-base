#!/bin/bash

{ \
	echo '#!/bin/bash'; \
	echo 'exec ${IGNIS_HOME}/backend/jre/bin/java -cp "${IGNIS_HOME}/lib/java/*" org.ignis.backend.Main "$@"'; \
} > ${IGNIS_HOME}/bin/ignis-backend
chmod +x ${IGNIS_HOME}/bin/ignis-backend
