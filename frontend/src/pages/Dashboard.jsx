import React, { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import api from '../services/api';
import { useAuth } from '../context/AuthContext';

const Dashboard = () => {
  const { user } = useAuth();
  const [stats, setStats] = useState(null);
  const [recentAnnonces, setRecentAnnonces] = useState([]);
  const [favoris, setFavoris] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (user) {
      loadDashboardData();
    }
  }, [user]);

  const loadDashboardData = async () => {
    try {
      setLoading(true);
      const [statsRes, annoncesRes, favorisRes] = await Promise.all([
        api.get('/statistiques/dashboard'),
        api.get('/annonces?limit=5'),
        api.get('/favoris')
      ]);

      setStats(statsRes.data);
      setRecentAnnonces(annoncesRes.data.annonces || []);
      setFavoris(favorisRes.data || []);
    } catch (error) {
      console.error('Erreur lors du chargement du dashboard:', error);
    } finally {
      setLoading(false);
    }
  };

  const formatPrix = (prix) => {
    return new Intl.NumberFormat('fr-FR').format(prix) + ' FCFA';
  };

  const formatPrixM2 = (prix) => {
    return new Intl.NumberFormat('fr-FR').format(prix) + ' FCFA/m²';
  };

  if (!user) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <h2 className="text-2xl font-bold text-gray-800 mb-4">
            Connexion requise
          </h2>
          <p className="text-gray-600 mb-6">
            Vous devez être connecté pour accéder à votre tableau de bord.
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

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="max-w-7xl mx-auto px-4 py-8">
        {/* En-tête */}
        <div className="mb-8">
          <h1 className="text-3xl font-bold text-[#065A82] mb-2">
            Bonjour {user.prenom} !
          </h1>
          <p className="text-gray-600">
            Bienvenue sur votre tableau de bord immobilier
          </p>
        </div>

        {/* Statistiques générales */}
        {stats && (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
            <div className="bg-white rounded-lg shadow-md p-6">
              <div className="flex items-center">
                <div className="p-3 bg-blue-100 rounded-full">
                  <span className="text-2xl">🏠</span>
                </div>
                <div className="ml-4">
                  <p className="text-sm text-gray-600">Total annonces</p>
                  <p className="text-2xl font-bold text-[#065A82]">
                    {stats.total_annonces?.toLocaleString() || 0}
                  </p>
                </div>
              </div>
            </div>

            <div className="bg-white rounded-lg shadow-md p-6">
              <div className="flex items-center">
                <div className="p-3 bg-green-100 rounded-full">
                  <span className="text-2xl">📍</span>
                </div>
                <div className="ml-4">
                  <p className="text-sm text-gray-600">Zones couvertes</p>
                  <p className="text-2xl font-bold text-[#065A82]">
                    {stats.zones_count || 0}
                  </p>
                </div>
              </div>
            </div>

            <div className="bg-white rounded-lg shadow-md p-6">
              <div className="flex items-center">
                <div className="p-3 bg-yellow-100 rounded-full">
                  <span className="text-2xl">💰</span>
                </div>
                <div className="ml-4">
                  <p className="text-sm text-gray-600">Prix moyen</p>
                  <p className="text-xl font-bold text-[#065A82]">
                    {stats.prix_moyen ? formatPrix(Math.round(stats.prix_moyen)) : 'N/A'}
                  </p>
                </div>
              </div>
            </div>

            <div className="bg-white rounded-lg shadow-md p-6">
              <div className="flex items-center">
                <div className="p-3 bg-purple-100 rounded-full">
                  <span className="text-2xl">📊</span>
                </div>
                <div className="ml-4">
                  <p className="text-sm text-gray-600">Prix/m² moyen</p>
                  <p className="text-xl font-bold text-[#065A82]">
                    {stats.prix_moyen_m2 ? formatPrixM2(Math.round(stats.prix_moyen_m2)) : 'N/A'}
                  </p>
                </div>
              </div>
            </div>
          </div>
        )}

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
          {/* Mes favoris */}
          <div className="bg-white rounded-lg shadow-md p-6">
            <div className="flex justify-between items-center mb-6">
              <h2 className="text-xl font-bold text-[#065A82]">
                Mes Favoris ({favoris.length})
              </h2>
              <Link
                to="/favoris"
                className="text-[#065A82] hover:text-opacity-80 text-sm font-medium"
              >
                Voir tout →
              </Link>
            </div>

            {favoris.length > 0 ? (
              <div className="space-y-4">
                {favoris.slice(0, 3).map(favori => (
                  <div key={favori.annonce_id} className="flex items-center space-x-4 p-3 border border-gray-200 rounded-lg">
                    <div className="flex-1">
                      <h3 className="font-medium text-gray-900 line-clamp-1">
                        {favori.annonce.titre}
                      </h3>
                      <p className="text-sm text-gray-600">
                        📍 {favori.annonce.zone} • {formatPrix(favori.annonce.prix)}
                      </p>
                    </div>
                    <Link
                      to={`/bien/${favori.annonce.id}`}
                      className="text-[#065A82] hover:text-opacity-80 text-sm"
                    >
                      Voir
                    </Link>
                  </div>
                ))}
              </div>
            ) : (
              <div className="text-center py-8">
                <p className="text-gray-500 mb-4">Aucun favori pour le moment</p>
                <Link
                  to="/recherche"
                  className="bg-[#065A82] text-white px-4 py-2 rounded hover:bg-opacity-90 transition-colors text-sm"
                >
                  Explorer les annonces
                </Link>
              </div>
            )}
          </div>

          {/* Annonces récentes */}
          <div className="bg-white rounded-lg shadow-md p-6">
            <div className="flex justify-between items-center mb-6">
              <h2 className="text-xl font-bold text-[#065A82]">
                Annonces Récentes
              </h2>
              <Link
                to="/recherche"
                className="text-[#065A82] hover:text-opacity-80 text-sm font-medium"
              >
                Voir tout →
              </Link>
            </div>

            {recentAnnonces.length > 0 ? (
              <div className="space-y-4">
                {recentAnnonces.map(annonce => (
                  <div key={annonce.id} className="flex items-center space-x-4 p-3 border border-gray-200 rounded-lg">
                    <div className="flex-1">
                      <h3 className="font-medium text-gray-900 line-clamp-1">
                        {annonce.titre}
                      </h3>
                      <p className="text-sm text-gray-600">
                        📍 {annonce.zone} • {formatPrix(annonce.prix)}
                      </p>
                      <p className="text-xs text-gray-500">
                        {annonce.type_bien} • {annonce.type_offre}
                      </p>
                    </div>
                    <Link
                      to={`/bien/${annonce.id}`}
                      className="text-[#065A82] hover:text-opacity-80 text-sm"
                    >
                      Voir
                    </Link>
                  </div>
                ))}
              </div>
            ) : (
              <div className="text-center py-8">
                <p className="text-gray-500">Aucune annonce récente</p>
              </div>
            )}
          </div>
        </div>

        {/* Actions rapides */}
        <div className="mt-8 bg-white rounded-lg shadow-md p-6">
          <h2 className="text-xl font-bold text-[#065A82] mb-6">
            Actions Rapides
          </h2>

          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
            <Link
              to="/recherche"
              className="flex items-center p-4 border border-gray-200 rounded-lg hover:bg-gray-50 transition-colors"
            >
              <div className="p-3 bg-[#065A82] bg-opacity-10 rounded-full mr-4">
                <span className="text-xl">🔍</span>
              </div>
              <div>
                <h3 className="font-medium text-gray-900">Rechercher</h3>
                <p className="text-sm text-gray-600">Trouver un bien</p>
              </div>
            </Link>

            <Link
              to="/simulateur"
              className="flex items-center p-4 border border-gray-200 rounded-lg hover:bg-gray-50 transition-colors"
            >
              <div className="p-3 bg-[#F59E0B] bg-opacity-10 rounded-full mr-4">
                <span className="text-xl">🧮</span>
              </div>
              <div>
                <h3 className="font-medium text-gray-900">Simulateur</h3>
                <p className="text-sm text-gray-600">Calculer un prêt</p>
              </div>
            </Link>

            <Link
              to="/indice"
              className="flex items-center p-4 border border-gray-200 rounded-lg hover:bg-gray-50 transition-colors"
            >
              <div className="p-3 bg-green-100 rounded-full mr-4">
                <span className="text-xl">📈</span>
              </div>
              <div>
                <h3 className="font-medium text-gray-900">Indices</h3>
                <p className="text-sm text-gray-600">Prix du marché</p>
              </div>
            </Link>

            <Link
              to="/favoris"
              className="flex items-center p-4 border border-gray-200 rounded-lg hover:bg-gray-50 transition-colors"
            >
              <div className="p-3 bg-red-100 rounded-full mr-4">
                <span className="text-xl">❤️</span>
              </div>
              <div>
                <h3 className="font-medium text-gray-900">Favoris</h3>
                <p className="text-sm text-gray-600">Mes sauvegardes</p>
              </div>
            </Link>
          </div>
        </div>

        {/* Profil utilisateur */}
        <div className="mt-8 bg-white rounded-lg shadow-md p-6">
          <h2 className="text-xl font-bold text-[#065A82] mb-6">
            Mon Profil
          </h2>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div>
              <h3 className="font-medium text-gray-900 mb-2">Informations personnelles</h3>
              <div className="space-y-2 text-sm">
                <p><span className="font-medium">Nom:</span> {user.nom} {user.prenom}</p>
                <p><span className="font-medium">Email:</span> {user.email}</p>
                <p><span className="font-medium">Rôle:</span> {user.role}</p>
                {user.date_creation && (
                  <p><span className="font-medium">Membre depuis:</span> {new Date(user.date_creation).toLocaleDateString('fr-FR')}</p>
                )}
              </div>
            </div>

            <div>
              <h3 className="font-medium text-gray-900 mb-2">Actions</h3>
              <div className="space-y-2">
                <Link
                  to="/profil"
                  className="block text-[#065A82] hover:text-opacity-80 text-sm"
                >
                  Modifier mon profil →
                </Link>
                <button
                  onClick={() => {/* TODO: implémenter la déconnexion */}}
                  className="block text-red-600 hover:text-red-800 text-sm"
                >
                  Se déconnecter
                </button>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default Dashboard;