#!/usr/bin/env bash
# Copyright (c) 2026 IS4IT
#
# Build the SAP connector bundle, copy it (and SAP JCo, if provided) into the
# running mp_server container, and restart it so the connector is picked up.
#
# Why not bind-mount? Single-file bind mounts on Colima / Docker Desktop / macOS
# are flaky once symlinks or named-volume nesting enter the picture. docker cp +
# restart is unambiguous and works the same on every host.
#
# SAP JCo (Linux build) is optional for the script but REQUIRED for the connector
# to actually instantiate. Place sapjco3.jar and libsapjco3.so in docker/jco/
# (see docker/README.md). They are copied into $MIDPOINT_HOME/lib.
#
# It also renders docker/connector-template.xml from src/test/resources/test.properties
# (single source of truth for the connection + table settings) and drops the result into
# $MIDPOINT_HOME/post-initial-objects/, so midPoint imports the SAP resource template on
# the (re)start below. Concrete test resources can then inherit from it via <super>.
#
# Pass --no-build to skip the Maven build.
#
# Usage (from any directory):
#   docker/deploy-connector.sh
#   docker/deploy-connector.sh --no-build

set -euo pipefail

# bash >=5.2 expands '&' in a ${var//pat/repl} replacement to the matched text, which
# would corrupt the XML escaping and token substitution below (any '&' in a value, e.g.
# the '&amp;'/'&lt;' we emit). Turn it off so '&' stays literal (no-op on older bash).
shopt -u patsub_replacement 2>/dev/null || true

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd -P)
PROJECT_DIR=$(cd "${SCRIPT_DIR}/.." && pwd -P)
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.yml"
SERVICE=mp_server
TARGET_CONNECTORS=/opt/midpoint/var/icf-connectors
TARGET_LIB=/opt/midpoint/var/lib

build=true
if [[ "${1:-}" == "--no-build" ]]; then
    build=false
fi

if $build; then
    echo "==> Building connector bundle"
    (cd "${PROJECT_DIR}" && mvn -ntp -B package -DskipTests)
fi

# The assembly (appendAssemblyId=false) makes target/connector-sap-<version>.jar
# the bundle, while connector-sap-<version>-bundle.jar would be a secondary one.
# Use -E (extended regex) for the alternation: BSD/macOS grep does not support the GNU \| operator.
BUNDLE=$(ls -1t "${PROJECT_DIR}"/target/connector-sap-*.jar 2>/dev/null | grep -vE -- '-(sources|javadoc)' | head -1)
if [[ -z "${BUNDLE}" ]]; then
    echo "ERROR: no built connector bundle found under ${PROJECT_DIR}/target/" >&2
    exit 1
fi
echo "==> Using ${BUNDLE}"

CONTAINER=$(docker compose -f "${COMPOSE_FILE}" ps -q "${SERVICE}")
if [[ -z "${CONTAINER}" ]]; then
    echo "ERROR: ${SERVICE} container is not running. Start the stack first:" >&2
    echo "       docker compose -f ${COMPOSE_FILE} up -d" >&2
    exit 1
fi

echo "==> Deploying connector bundle into ${TARGET_CONNECTORS}"
docker exec "${CONTAINER}" sh -c "mkdir -p '${TARGET_CONNECTORS}' && rm -f '${TARGET_CONNECTORS}'/connector-sap-*.jar"
docker cp "${BUNDLE}" "${CONTAINER}:${TARGET_CONNECTORS}/$(basename "${BUNDLE}")"

# SAP JCo (Linux): jar onto the classpath, native lib where LD_LIBRARY_PATH points.
JCO_DIR="${SCRIPT_DIR}/jco"
if [[ -f "${JCO_DIR}/sapjco3.jar" ]]; then
    echo "==> Deploying SAP JCo jar into ${TARGET_LIB}"
    docker exec "${CONTAINER}" sh -c "mkdir -p '${TARGET_LIB}'"
    docker cp "${JCO_DIR}/sapjco3.jar" "${CONTAINER}:${TARGET_LIB}/sapjco3.jar"
else
    echo "WARN: ${JCO_DIR}/sapjco3.jar not found - the SAP connector will not load until JCo is provided (see docker/README.md)" >&2
fi
if [[ -f "${JCO_DIR}/libsapjco3.so" ]]; then
    echo "==> Deploying SAP JCo native library into ${TARGET_LIB}"
    docker cp "${JCO_DIR}/libsapjco3.so" "${CONTAINER}:${TARGET_LIB}/libsapjco3.so"
else
    echo "WARN: ${JCO_DIR}/libsapjco3.so (Linux native lib) not found - SAP connectivity will fail until provided" >&2
fi

# --- Resource template: merge test.properties into the template, stage for import ----
# midPoint imports objects placed in $MIDPOINT_HOME/post-initial-objects at startup, so the
# restart below makes the rendered template available. Every key in test.properties becomes a
# <cfg:KEY> connector configuration property (single source of truth, not duplicated in XML):
#   - keys starting with "test." are test-harness only and skipped
#   - r3name maps to the connector's "systemId" property
#   - password is emitted as a clearValue (midPoint encrypts it on import)
#   - a value containing ';' is multi-valued and becomes repeated <cfg:KEY> elements
# Generated properties replace same-named defaults in the template; extra ones are appended.
TEMPLATE="${SCRIPT_DIR}/connector-template.xml"
PROPS="${PROJECT_DIR}/src/test/resources/test.properties"
TARGET_POST_INITIAL=/opt/midpoint/var/post-initial-objects
POST_INITIAL_FILE=sap-connector-template.xml
CONFIG_TOKEN='@SAP_CONFIG_PROPERTIES@'   # marker in the template where generated props go

trim() { local s=$1; s="${s#"${s%%[![:space:]]*}"}"; s="${s%"${s##*[![:space:]]}"}"; printf '%s' "$s"; }
xml_escape() { local s=$1; s=${s//&/&amp;}; s=${s//</&lt;}; s=${s//>/&gt;}; printf '%s' "$s"; }

if [[ ! -f "${TEMPLATE}" ]]; then
    echo "WARN: ${TEMPLATE} missing - skipping resource template deployment" >&2
elif [[ ! -f "${PROPS}" ]]; then
    echo "WARN: ${PROPS} missing - cannot render the resource template, skipping it" >&2
else
    declare -a gen_lines=() del_args=()
    declare -A have=()
    while IFS= read -r line || [[ -n "${line}" ]]; do
        line=${line%$'\r'}
        [[ "${line}" =~ ^[[:space:]]*# ]] && continue   # comment
        [[ "${line}" != *=* ]] && continue              # not a key=value line
        key=$(trim "${line%%=*}")
        val=$(trim "${line#*=}")
        [[ -z "${key}" || -z "${val}" ]] && continue    # skip empty key/value
        [[ "${key}" == test.* ]] && continue            # test-harness-only key
        cfg="${key}"; [[ "${cfg}" == "r3name" ]] && cfg="systemId"   # property-name mapping
        have["${cfg}"]=1
        if [[ "${cfg}" == "password" ]]; then
            del_args+=(-e "/<cfg:password>/,/<\/cfg:password>/d")
            gen_lines+=("            <cfg:password>")
            gen_lines+=("                <t:clearValue>$(xml_escape "${val}")</t:clearValue>")
            gen_lines+=("            </cfg:password>")
        else
            del_args+=(-e "/<cfg:${cfg}>/d")
            IFS=';' read -r -a segs <<< "${val}"        # ';' = multi-value delimiter
            for seg in "${segs[@]}"; do
                seg=$(trim "${seg}"); [[ -z "${seg}" ]] && continue
                gen_lines+=("            <cfg:${cfg}>$(xml_escape "${seg}")</cfg:${cfg}>")
            done
        fi
    done < "${PROPS}"

    # a usable resource needs at least these (systemId originates from r3name)
    missing=""
    for need in host systemNumber systemId client user password; do
        [[ -z "${have[${need}]:-}" ]] && missing+=" ${need}"
    done
    missing=${missing/ systemId/ r3name}

    if [[ -n "${missing}" ]]; then
        echo "WARN: test.properties has no value for:${missing} - skipping resource template deployment" >&2
    else
        gen=$(printf '%s\n' "${gen_lines[@]}"); gen=${gen%$'\n'}
        # drop any template default that test.properties overrides, then inject generated props
        if ((${#del_args[@]})); then
            rendered=$(sed "${del_args[@]}" "${TEMPLATE}")
        else
            rendered=$(cat "${TEMPLATE}")
        fi
        rendered=${rendered//${CONFIG_TOKEN}/$gen}
        if [[ "${rendered}" == *"${CONFIG_TOKEN}"* ]]; then
            echo "WARN: ${CONFIG_TOKEN} marker not found/replaced in ${TEMPLATE} - check the template" >&2
        fi

        tmp_render=$(mktemp)
        printf '%s\n' "${rendered}" > "${tmp_render}"
        echo "==> Deploying resource template into ${TARGET_POST_INITIAL} (${#have[@]} cfg properties from test.properties)"
        docker exec "${CONTAINER}" sh -c "mkdir -p '${TARGET_POST_INITIAL}'"
        docker cp "${tmp_render}" "${CONTAINER}:${TARGET_POST_INITIAL}/${POST_INITIAL_FILE}"
        rm -f "${tmp_render}"
    fi
fi

echo "==> Restarting ${SERVICE}"
docker compose -f "${COMPOSE_FILE}" restart "${SERVICE}"

echo "==> Done. Tail logs with:"
echo "    docker compose -f ${COMPOSE_FILE} logs -f ${SERVICE}"
