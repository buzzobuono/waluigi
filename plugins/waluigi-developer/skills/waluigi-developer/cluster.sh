#!/data/data/com.termux/files/usr/bin/bash
set -e

LOGS="$HOME/wlprj/logs"
PIDS="$HOME/wlprj/.cluster.pids"

start() {
  if [ -f "$PIDS" ]; then
    echo "Cluster già avviato (rimuovi $PIDS per forzare)"
    exit 1
  fi

  mkdir -p "$LOGS"
  echo "Avvio cluster..."

  wlcatalog > "$LOGS/wlcatalog.log" 2>&1 & echo $! >> "$PIDS"
  echo "  wlcatalog avviato (PID $!)"
  sleep 2

  wlboss > "$LOGS/wlboss.log" 2>&1 & echo $! >> "$PIDS"
  echo "  wlboss avviato (PID $!)"
  sleep 2

  wlconsole > "$LOGS/wlconsole.log" 2>&1 & echo $! >> "$PIDS"
  echo "  wlconsole avviato (PID $!)"
  sleep 2

  wlworker --boss-url http://localhost:8082 --port 5001 --slots 4 --affinity python \
    > "$LOGS/wlworker.log" 2>&1 & echo $! >> "$PIDS"
  echo "  wlworker avviato (PID $!)"
  sleep 3

  wlctl --url http://localhost:8080 login -u admin -p admin > /dev/null 2>&1
  echo ""
  wlctl get workers
  echo ""
  echo "Cluster pronto → http://localhost:8080"
}

stop() {
  if [ ! -f "$PIDS" ]; then
    echo "Nessun cluster in esecuzione (file $PIDS non trovato)"
    exit 0
  fi

  echo "Arresto cluster..."
  while IFS= read -r pid; do
    if kill "$pid" 2>/dev/null; then
      echo "  Terminato PID $pid"
    fi
  done < "$PIDS"

  rm -f "$PIDS"
  echo "Cluster fermato."
}

status() {
  if [ ! -f "$PIDS" ]; then
    echo "Cluster non avviato."
    exit 0
  fi
  wlctl get workers 2>&1 || echo "Console non raggiungibile"
}

logs() {
  local component="${2:-all}"
  if [ "$component" = "all" ]; then
    tail -f "$LOGS"/wl*.log
  else
    tail -f "$LOGS/wl${component}.log"
  fi
}

case "$1" in
  start)  start ;;
  stop)   stop ;;
  status) status ;;
  logs)   logs "$@" ;;
  restart) stop; sleep 1; start ;;
  *)
    echo "Uso: $0 {start|stop|restart|status|logs [catalog|boss|console|worker]}"
    exit 1
    ;;
esac
