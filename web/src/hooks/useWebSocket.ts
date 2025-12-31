import { useEffect, useState } from 'react';
import { wsManager } from '../api/websocket';
import type { ProgressUpdate } from '../types/api';

export function useWebSocket() {
  const [isConnected, setIsConnected] = useState(false);
  const [updates, setUpdates] = useState<ProgressUpdate[]>([]);

  useEffect(() => {
    // Connect WebSocket
    wsManager.connect();
    setIsConnected(true);

    // Subscribe to updates
    const unsubscribe = wsManager.subscribe((update) => {
      setUpdates((prev) => [...prev, update]);
    });

    // Cleanup
    return () => {
      unsubscribe();
    };
  }, []);

  return { isConnected, updates };
}
