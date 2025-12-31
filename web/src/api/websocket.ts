import type { ProgressUpdate } from '../types/api';

export class WebSocketManager {
  private ws: WebSocket | null = null;
  private listeners = new Set<(update: ProgressUpdate) => void>();
  private reconnectTimeout: number | null = null;
  private url: string;

  constructor() {
    // Determine WebSocket URL based on current location
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const host = window.location.host;
    this.url = `${protocol}//${host}/ws`;
  }

  connect() {
    if (this.ws?.readyState === WebSocket.OPEN) {
      return; // Already connected
    }

    try {
      this.ws = new WebSocket(this.url);

      this.ws.onopen = () => {
        console.log('WebSocket connected');
      };

      this.ws.onmessage = (event) => {
        try {
          const update: ProgressUpdate = JSON.parse(event.data);
          this.listeners.forEach(fn => fn(update));
        } catch (error) {
          console.error('Failed to parse WebSocket message:', error);
        }
      };

      this.ws.onerror = (error) => {
        console.error('WebSocket error:', error);
      };

      this.ws.onclose = () => {
        console.log('WebSocket disconnected, reconnecting in 3s...');
        this.reconnectTimeout = window.setTimeout(() => this.connect(), 3000);
      };
    } catch (error) {
      console.error('Failed to create WebSocket:', error);
      this.reconnectTimeout = window.setTimeout(() => this.connect(), 3000);
    }
  }

  subscribe(callback: (update: ProgressUpdate) => void) {
    this.listeners.add(callback);
    return () => this.listeners.delete(callback);
  }

  disconnect() {
    if (this.reconnectTimeout) {
      clearTimeout(this.reconnectTimeout);
      this.reconnectTimeout = null;
    }
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
  }
}

// Singleton instance
export const wsManager = new WebSocketManager();
