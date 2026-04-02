import React, { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import { api } from '../services/api';
import { useAuth } from '../context/AuthContext';
import AnnonceCard from '../components/AnnonceCard';

const Favoris = () => {
  const { user } = useAuth();
  const [favoris, setFavoris] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (user) {
      loadFavoris();
    } else {
      setLoading(false);
    }
  }, [user]);

  const loadFavoris = async () => {
    try {
      setLoading(true);
      const response = await api.get('/favoris');
      setFavoris(response.data || []);
    } catch (error) {
      console.error('Erreur lors du chargement des favoris:', error);
      setError('Erreur lors du chargement des favoris');
    } finally {
      setLoading(false);
    }
  };

  const toggleFavori = async (annonceId) => {
    try {
      await api.delete(`/favoris/${annonceId}`);
      // Recharger la liste après suppression
      loadFavoris();
    } catch (error) {
      console.error('Erreur lors de la suppression des favoris:', error);
    }
  };

  if (!user) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <h2 className="text-2xl font-bold text-gray-800 mb-4">
            Connexion requise
          </h2>
          <p className="text-gray-600 mb-6">
            Vous devez être connecté pour accéder à vos favoris.
          </p>
          <Link
            to="/login"
            className="bg-[#065A82] text-white px-6 py-3 rounded hover:bg-opacity-90 transition-colors"
          >
            Se connecter
          </Link>
        </div>
      </div>
    );
  }

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-[#065A82]"></div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <h2 className="text-2xl font-bold text-gray-800 mb-4">Erreur</h2>
          <p className="text-gray-600 mb-6">{error}</p>
          <button
            onClick={loadFavoris}
            className="bg-[#065A82] text-white px-6 py-3 rounded hover:bg-opacity-90 transition-colors"
          >
            Réessayer
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="max-w-7xl mx-auto px-4 py-8">
        {/* Titre */}
        <div className="mb-8">
          <h1 className="text-3xl font-bold text-[#065A82] mb-2">
            Mes Favoris
          </h1>
          <p className="text-gray-600">
            {favoris.length} annonce{favoris.length !== 1 ? 's' : ''} sauvegardée{favoris.length !== 1 ? 's' : ''}
          </p>
        </div>

        {/* Liste des favoris */}
        {favoris.length > 0 ? (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {favoris.map(favori => (
              <AnnonceCard
                key={favori.annonce_id}
                annonce={favori.annonce}
                onToggleFavori={toggleFavori}
              />
            ))}
          </div>
        ) : (
          <div className="text-center py-16 bg-white rounded-lg shadow-md">
            <div className="mb-6">
              <span className="text-6xl">❤️</span>
            </div>
            <h2 className="text-2xl font-bold text-gray-800 mb-4">
              Aucun favori pour le moment
            </h2>
            <p className="text-gray-600 mb-6 max-w-md mx-auto">
              Vous n'avez pas encore ajouté d'annonces à vos favoris.
              Parcourez les annonces disponibles et cliquez sur le cœur pour les sauvegarder.
            </p>
            <Link
              to="/recherche"
              className="inline-block bg-[#065A82] text-white px-6 py-3 rounded hover:bg-opacity-90 transition-colors"
            >
              Découvrir les annonces
            </Link>
          </div>
        )}

        {/* Statistiques des favoris */}
        {favoris.length > 0 && (
          <div className="mt-12 bg-white rounded-lg shadow-md p-6">
            <h2 className="text-xl font-bold text-[#065A82] mb-6">
              Statistiques de vos favoris
            </h2>

            <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
              {/* Prix moyen */}
              <div className="text-center">
                <div className="text-3xl font-bold text-[#065A82] mb-2">
                  {favoris.length > 0
                    ? formatPrix(
                        favoris.reduce((sum, f) => sum + f.annonce.prix, 0) / favoris.length
                      )
                    : '0 FCFA'
                  }
                </div>
                <div className="text-sm text-gray-600">Prix moyen</div>
              </div>

              {/* Zones favorites */}
              <div className="text-center">
                <div className="text-3xl font-bold text-[#065A82] mb-2">
                  {new Set(favoris.map(f => f.annonce.zone)).size}
                </div>
                <div className="text-sm text-gray-600">Zone(s) différente(s)</div>
              </div>

              {/* Types de biens */}
              <div className="text-center">
                <div className="text-3xl font-bold text-[#065A82] mb-2">
                  {new Set(favoris.map(f => f.annonce.type_bien)).size}
                </div>
                <div className="text-sm text-gray-600">Type(s) de bien(s)</div>
              </div>
            </div>

            {/* Répartition par zone */}
            <div className="mt-8">
              <h3 className="text-lg font-semibold text-gray-800 mb-4">
                Répartition par zone
              </h3>
              <div className="space-y-2">
                {Object.entries(
                  favoris.reduce((acc, f) => {
                    acc[f.annonce.zone] = (acc[f.annonce.zone] || 0) + 1;
                    return acc;
                  }, {})
                ).map(([zone, count]) => (
                  <div key={zone} className="flex justify-between items-center">
                    <span className="text-gray-700">{zone}</span>
                    <div className="flex items-center space-x-2">
                      <div className="w-24 bg-gray-200 rounded-full h-2">
                        <div
                          className="bg-[#065A82] h-2 rounded-full"
                          style={{ width: `${(count / favoris.length) * 100}%` }}
                        ></div>
                      </div>
                      <span className="text-sm text-gray-600 w-8 text-right">
                        {count}
                      </span>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </div>
        )}

        {/* Actions */}
        <div className="mt-8 text-center">
          <Link
            to="/recherche"
            className="bg-[#065A82] text-white px-6 py-3 rounded hover:bg-opacity-90 transition-colors"
          >
            Continuer mes recherches
          </Link>
        </div>
      </div>
    </div>
  );
};

// Fonction utilitaire pour formater les prix
const formatPrix = (prix) => {
  return new Intl.NumberFormat('fr-FR').format(Math.round(prix)) + ' FCFA';
};

export default Favoris;