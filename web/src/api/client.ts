// HTTP client for API requests

const API_BASE = import.meta.env.VITE_API_URL || 'http://localhost:8080';

class APIError extends Error {
  constructor(
    message: string,
    public status: number,
    public statusText: string
  ) {
    super(message);
    this.name = 'APIError';
  }
}

export const apiClient = {
  get: async <T>(endpoint: string): Promise<T> => {
    const res = await fetch(`${API_BASE}${endpoint}`);

    if (!res.ok) {
      throw new APIError(
        `GET ${endpoint} failed: ${res.statusText}`,
        res.status,
        res.statusText
      );
    }

    return res.json();
  },

  post: async <T>(endpoint: string, data: unknown): Promise<T> => {
    const res = await fetch(`${API_BASE}${endpoint}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(data),
    });

    if (!res.ok) {
      throw new APIError(
        `POST ${endpoint} failed: ${res.statusText}`,
        res.status,
        res.statusText
      );
    }

    return res.json();
  },
};
