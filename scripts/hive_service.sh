#!/usr/bin/env bash
set -euo pipefail

LABEL="com.tradingai.hive"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
PLIST_DIR="${HOME}/Library/LaunchAgents"
PLIST_PATH="${PLIST_DIR}/${LABEL}.plist"
LOG_DIR="${PROJECT_ROOT}/logs"
OUT_LOG="${LOG_DIR}/hive.launchd.out.log"
ERR_LOG="${LOG_DIR}/hive.launchd.err.log"
PYTHON_BIN="${PROJECT_ROOT}/.venv/bin/python"
RUNNER="${PROJECT_ROOT}/run_hive.py"
GUI_DOMAIN="gui/$(id -u)"

print_usage() {
  cat <<EOF
Usage: scripts/hive_service.sh <command>

Commands:
  install    Write/refresh launchd plist only
  start      Install plist and start service
  stop       Stop service
  restart    Restart service
  status     Show service status
  screenoff  Turn display off now (service keeps running)
  logs       Tail service logs
  uninstall  Stop service and remove plist
EOF
}

require_runtime() {
  if [[ ! -x "${PYTHON_BIN}" ]]; then
    echo "Missing virtualenv python at ${PYTHON_BIN}"
    echo "Create it with: python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt"
    exit 1
  fi
  if [[ ! -f "${RUNNER}" ]]; then
    echo "Missing runner: ${RUNNER}"
    exit 1
  fi
}

write_plist() {
  mkdir -p "${PLIST_DIR}" "${LOG_DIR}"

  cat >"${PLIST_PATH}" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>${LABEL}</string>

  <key>ProgramArguments</key>
  <array>
    <string>/usr/bin/caffeinate</string>
    <string>-ims</string>
    <string>${PYTHON_BIN}</string>
    <string>${RUNNER}</string>
  </array>

  <key>WorkingDirectory</key>
  <string>${PROJECT_ROOT}</string>

  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>

  <key>EnvironmentVariables</key>
  <dict>
    <key>PYTHONUNBUFFERED</key>
    <string>1</string>
  </dict>

  <!-- Ensure bundled CA bundle is used so websocket/http clients can validate certificates -->
  <key>EnvironmentVariables</key>
  <dict>
    <key>PYTHONUNBUFFERED</key>
    <string>1</string>
    <key>SSL_CERT_FILE</key>
    <string>/Users/pardeepkumarmaheshwari/Desktop/TradingAI/.venv/lib/python3.12/site-packages/certifi/cacert.pem</string>
  </dict>

  <key>StandardOutPath</key>
  <string>${OUT_LOG}</string>
  <key>StandardErrorPath</key>
  <string>${ERR_LOG}</string>
</dict>
</plist>
EOF

  echo "Wrote ${PLIST_PATH}"
}

stop_service() {
  if launchctl print "${GUI_DOMAIN}/${LABEL}" >/dev/null 2>&1; then
    launchctl bootout "${GUI_DOMAIN}/${LABEL}" >/dev/null
    echo "Stopped ${LABEL}"
  else
    echo "${LABEL} is not running"
  fi
}

start_service() {
  require_runtime
  write_plist

  if launchctl print "${GUI_DOMAIN}/${LABEL}" >/dev/null 2>&1; then
    launchctl bootout "${GUI_DOMAIN}/${LABEL}" >/dev/null || true
  fi

  launchctl bootstrap "${GUI_DOMAIN}" "${PLIST_PATH}"
  launchctl kickstart -k "${GUI_DOMAIN}/${LABEL}"
  echo "Started ${LABEL}"
}

service_status() {
  if launchctl print "${GUI_DOMAIN}/${LABEL}" >/dev/null 2>&1; then
    local pid
    pid="$(launchctl print "${GUI_DOMAIN}/${LABEL}" | awk '/pid =/ {print $3; exit}')"
    echo "${LABEL} is running (pid ${pid:-unknown})"
    echo "stdout: ${OUT_LOG}"
    echo "stderr: ${ERR_LOG}"
  else
    echo "${LABEL} is not running"
  fi
}

tail_logs() {
  mkdir -p "${LOG_DIR}"
  touch "${OUT_LOG}" "${ERR_LOG}"
  tail -n 50 -f "${OUT_LOG}" "${ERR_LOG}"
}

sleep_display_now() {
  pmset displaysleepnow
}

uninstall_service() {
  stop_service
  rm -f "${PLIST_PATH}"
  echo "Removed ${PLIST_PATH}"
}

cmd="${1:-}"
case "${cmd}" in
  install)
    require_runtime
    write_plist
    ;;
  start)
    start_service
    ;;
  stop)
    stop_service
    ;;
  restart)
    stop_service
    start_service
    ;;
  status)
    service_status
    ;;
  screenoff)
    sleep_display_now
    ;;
  logs)
    tail_logs
    ;;
  uninstall)
    uninstall_service
    ;;
  *)
    print_usage
    exit 1
    ;;
esac
