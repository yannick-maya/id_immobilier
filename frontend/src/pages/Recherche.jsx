import React, { useState, useEffect } from 'react';
import { useSearchParams } from 'react-router-dom';
import AnnonceCard from '../components/AnnonceCard';
import api from '../services/api';
import { useAuth } from '../context/AuthContext';

const Recherche = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const { user } = useAuth();

  // États pour les filtres
  const [filtres, setFiltres] = useState({
    zone: searchParams.get('zone') || '',
    type_bien: searchParams.get('type_bien') || '',
    type_offre: searchParams.get('type_offre') || '',
    prix_min: searchParams.get('prix_min') || '',
    prix_max: searchParams.get('prix_max') || '',
    surface_min: searchParams.get('surface_min') || '',
    surface_max: searchParams.get('surface_max') || '',
    pieces: searchParams.get('pieces') || '',
    source: searchParams.get('source') || ''
  });

  // États pour les données
  const [annonces, setAnnonces] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [limit] = useState(12);

  // États pour les options de filtres
  const [zones, setZones] = useState([]);
  const [typesBien, setTypesBien] = useState([]);
  const [sources, setSources] = useState([]);

  // Charger les options de filtres au montage
  useEffect(() => {
    loadFilterOptions();
  }, []);

  // Charger les annonces quand les filtres changent
  useEffect(() => {
    loadAnnonces();
    updateURL();
  }, [filtres, page]);

  const loadFilterOptions = async () => {
    try {
      const [zonesRes, typesRes, sourcesRes] = await Promise.all([
        api.get('/statistiques/zones'),
        api.get('/statistiques/types-bien'),
        api.get('/statistiques/sources')
      ]);

      setZones(zonesRes.data || []);
      setTypesBien(typesRes.data || []);
      setSources(sourcesRes.data || []);
    } catch (error) {
      console.error('Erreur lors du chargement des options:', error);
    }
  };

  const loadAnnonces = async () => {
    setLoading(true);
    setError(null);

    try {
      const params = {
        ...filtres,
        page,
        limit
      };

      // Supprimer les paramètres vides
      Object.keys(params).forEach(key => {
        if (params[key] === '') {
          delete params[key];
        }
      });

      const response = await api.get('/annonces', { params });
      setAnnonces(response.data.annonces || []);
      setTotal(response.data.total || 0);
    } catch (error) {
      console.error('Erreur lors du chargement des annonces:', error);
      setError('Erreur lors du chargement des annonces');
    } finally {
      setLoading(false);
    }
  };

  const updateURL = () => {
    const params = new URLSearchParams();
    Object.entries(filtres).forEach(([key, value]) => {
      if (value) {
        params.set(key, value);
      }
    });
    if (page > 1) {
      params.set('page', page);
    }
    setSearchParams(params);
  };

  const handleFiltreChange = (key, value) => {
    setFiltres(prev => ({ ...prev, [key]: value }));
    setPage(1); // Reset à la première page
  };

  const resetFiltres = () => {
    setFiltres({
      zone: '',
      type_bien: '',
      type_offre: '',
      prix_min: '',
      prix_max: '',
      surface_min: '',
      surface_max: '',
      pieces: '',
      source: ''
    });
    setPage(1);
  };

  const toggleFavori = async (annonceId) => {
    if (!user) return;

    try {
      await api.post(`/favoris/${annonceId}`);
      // Recharger les annonces pour mettre à jour l'état des favoris
      loadAnnonces();
    } catch (error) {
      console.error('Erreur lors de l\'ajout aux favoris:', error);
    }
  };

  const totalPages = Math.ceil(total / limit);

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="max-w-7xl mx-auto px-4 py-8">
        {/* Titre */}
        <div className="mb-8">
          <h1 className="text-3xl font-bold text-[#065A82] mb-2">
            Recherche d'annonces immobilières
          </h1>
          <p className="text-gray-600">
            Trouvez votre bien idéal parmi {total.toLocaleString()} annonces
          </p>
        </div>

        {/* Filtres */}
        <div className="bg-white rounded-lg shadow-md p-6 mb-8">
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
            {/* Zone */}
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Zone
              </label>
              <select
                value={filtres.zone}
                onChange={(e) => handleFiltreChange('zone', e.target.value)}
                className="w-full border border-gray-300 rounded-md px-3 py-2 focus:outline-none focus:ring-2 focus:ring-[#065A82]"
              >
                <option value="">Toutes les zones</option>
                {zones.map(zone => (
                  <option key={zone} value={zone}>{zone}</option>
                ))}
              </select>
            </div>

            {/* Type de bien */}
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Type de bien
              </label>
              <select
                value={filtres.type_bien}
                onChange={(e) => handleFiltreChange('type_bien', e.target.value)}
                className="w-full border border-gray-300 rounded-md px-3 py-2 focus:outline-none focus:ring-2 focus:ring-[#065A82]"
              >
                <option value="">Tous types</option>
                {typesBien.map(type => (
                  <option key={type} value={type}>{type}</option>
                ))}
              </select>
            </div>

            {/* Type d'offre */}
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Type d'offre
              </label>
              <select
                value={filtres.type_offre}
                onChange={(e) => handleFiltreChange('type_offre', e.target.value)}
                className="w-full border border-gray-300 rounded-md px-3 py-2 focus:outline-none focus:ring-2 focus:ring-[#065A82]"
              >
                <option value="">Toutes offres</option>
                <option value="Vente">Vente</option>
                <option value="Location">Location</option>
              </select>
            </div>

            {/* Source */}
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Source
              </label>
              <select
                value={filtres.source}
                onChange={(e) => handleFiltreChange('source', e.target.value)}
                className="w-full border border-gray-300 rounded-md px-3 py-2 focus:outline-none focus:ring-2 focus:ring-[#065A82]"
              >
                <option value="">Toutes sources</option>
                {sources.map(source => (
                  <option key={source} value={source}>{source}</option>
                ))}
              </select>
            </div>

            {/* Prix min */}
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Prix minimum (FCFA)
              </label>
              <input
                type="number"
                value={filtres.prix_min}
                onChange={(e) => handleFiltreChange('prix_min', e.target.value)}
                placeholder="Prix min"
                className="w-full border border-gray-300 rounded-md px-3 py-2 focus:outline-none focus:ring-2 focus:ring-[#065A82]"
              />
            </div>

            {/* Prix max */}
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Prix maximum (FCFA)
              </label>
              <input
                type="number"
                value={filtres.prix_max}
                onChange={(e) => handleFiltreChange('prix_max', e.target.value)}
                placeholder="Prix max"
                className="w-full border border-gray-300 rounded-md px-3 py-2 focus:outline-none focus:ring-2 focus:ring-[#065A82]"
              />
            </div>

            {/* Surface min */}
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Surface min (m²)
              </label>
              <input
                type="number"
                value={filtres.surface_min}
                onChange={(e) => handleFiltreChange('surface_min', e.target.value)}
                placeholder="Surface min"
                className="w-full border border-gray-300 rounded-md px-3 py-2 focus:outline-none focus:ring-2 focus:ring-[#065A82]"
              />
            </div>

            {/* Surface max */}
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Surface max (m²)
              </label>
              <input
                type="number"
                value={filtres.surface_max}
                onChange={(e) => handleFiltreChange('surface_max', e.target.value)}
                placeholder="Surface max"
                className="w-full border border-gray-300 rounded-md px-3 py-2 focus:outline-none focus:ring-2 focus:ring-[#065A82]"
              />
            </div>

            {/* Pièces */}
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Nombre de pièces
              </label>
              <select
                value={filtres.pieces}
                onChange={(e) => handleFiltreChange('pieces', e.target.value)}
                className="w-full border border-gray-300 rounded-md px-3 py-2 focus:outline-none focus:ring-2 focus:ring-[#065A82]"
              >
                <option value="">Toutes pièces</option>
                <option value="1">1 pièce</option>
                <option value="2">2 pièces</option>
                <option value="3">3 pièces</option>
                <option value="4">4 pièces</option>
                <option value="5+">5+ pièces</option>
              </select>
            </div>

            {/* Bouton reset */}
            <div className="flex items-end">
              <button
                onClick={resetFiltres}
                className="w-full bg-gray-500 text-white px-4 py-2 rounded hover:bg-gray-600 transition-colors"
              >
                Réinitialiser
              </button>
            </div>
          </div>
        </div>

        {/* Résultats */}
        {loading ? (
          <div className="text-center py-12">
            <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-[#065A82] mx-auto"></div>
            <p className="mt-4 text-gray-600">Chargement des annonces...</p>
          </div>
        ) : error ? (
          <div className="text-center py-12">
            <p className="text-red-600">{error}</p>
          </div>
        ) : (
          <>
            {/* Grille d'annonces */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 mb-8">
              {annonces.map(annonce => (
                <AnnonceCard
                  key={annonce.id}
                  annonce={annonce}
                  onToggleFavori={toggleFavori}
                />
              ))}
            </div>

            {/* Pagination */}
            {totalPages > 1 && (
              <div className="flex justify-center items-center space-x-2">
                <button
                  onClick={() => setPage(prev => Math.max(1, prev - 1))}
                  disabled={page === 1}
                  className="px-4 py-2 border border-gray-300 rounded-md hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  Précédent
                </button>

                <span className="text-gray-600">
                  Page {page} sur {totalPages}
                </span>

                <button
                  onClick={() => setPage(prev => Math.min(totalPages, prev + 1))}
                  disabled={page === totalPages}
                  className="px-4 py-2 border border-gray-300 rounded-md hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  Suivant
                </button>
              </div>
            )}

            {/* Message si aucune annonce */}
            {annonces.length === 0 && !loading && (
              <div className="text-center py-12">
                <p className="text-gray-600 text-lg">
                  Aucune annonce trouvée avec ces critères.
                </p>
                <p className="text-gray-500 mt-2">
                  Essayez de modifier vos filtres de recherche.
                </p>
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
};

export default Recherche;