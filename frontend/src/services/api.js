import axios from 'axios';

const API_URL = process.env.REACT_APP_API_URL || 'http://localhost:8000';

const api = axios.create({
  baseURL: API_URL,
});

// Intercepteur pour ajouter le token Bearer
api.interceptors.request.use(config => {
  const token = localStorage.getItem('token');
  if (token) {
    config.headers.Authorization = `Bearer ${token}`;
  }
  return config;
});

export default api;

export const authService = {
  register: (data) => api.post('/auth/register', data),
  login: (data) => api.post('/auth/login', data),
  me: () => api.get('/auth/me'),
};

export const annoncesService = {
  getAll: (params) => api.get('/annonces', { params }),
  getById: (id) => api.get(`/annonces/${id}`),
  search: (q) => api.get('/annonces/search', { params: { q } }),
};

export const indiceService = {
  getAll: (params) => api.get('/indice', { params }),
  getTendances: () => api.get('/indice/tendances'),
  getZone: (zone) => api.get(`/indice/${zone}`),
};