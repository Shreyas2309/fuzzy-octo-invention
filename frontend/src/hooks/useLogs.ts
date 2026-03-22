import { useEffect, useRef, useState, useCallback } from 'react';

export interface LogEntry {
  raw: string;
  ts?: string;
  level?: string;
  event?: string;
  logger?: string;
  extra?: Record<string, unknown>;
}

export function useLogs(maxLines = 500) {
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [connected, setConnected] = useState(false);
  const wsRef = useRef<WebSocket | null>(null);
  const pausedRef = useRef(false);
  const reconnectTimer = useRef<ReturnType<typeof setTimeout>>();

  useEffect(() => {
    let disposed = false;
    let backoff = 1000;

    function connect() {
      if (disposed) return;

      const proto = window.location.protocol === 'https:' ? 'wss' : 'ws';
      const wsUrl = `${proto}://${window.location.host}/ws/logs`;
      const ws = new WebSocket(wsUrl);
      wsRef.current = ws;

      ws.onopen = () => {
        setConnected(true);
        backoff = 1000; // reset on success
      };

      ws.onclose = () => {
        setConnected(false);
        if (!disposed) {
          reconnectTimer.current = setTimeout(connect, backoff);
          backoff = Math.min(backoff * 1.5, 10000);
        }
      };

      ws.onerror = () => {
        // onclose will fire after this, triggering reconnect
      };

      ws.onmessage = (event) => {
        if (pausedRef.current) return;
        try {
          const parsed = JSON.parse(event.data);
          const entry: LogEntry = {
            raw: event.data,
            ts: parsed.ts || parsed.timestamp,
            level: parsed.level,
            event: parsed.event,
            logger: parsed.logger,
            extra: parsed,
          };
          setLogs((prev) => {
            const next = [...prev, entry];
            return next.length > maxLines ? next.slice(-maxLines) : next;
          });
        } catch {
          setLogs((prev) => {
            const next = [...prev, { raw: event.data }];
            return next.length > maxLines ? next.slice(-maxLines) : next;
          });
        }
      };
    }

    connect();

    return () => {
      disposed = true;
      clearTimeout(reconnectTimer.current);
      wsRef.current?.close();
    };
  }, [maxLines]);

  const setPaused = useCallback((p: boolean) => {
    pausedRef.current = p;
  }, []);

  const clear = useCallback(() => setLogs([]), []);

  const sendFilter = useCallback((filter: string | null) => {
    if (wsRef.current?.readyState === WebSocket.OPEN) {
      wsRef.current.send(JSON.stringify({ filter }));
    }
  }, []);

  return { logs, connected, setPaused, clear, sendFilter };
}
