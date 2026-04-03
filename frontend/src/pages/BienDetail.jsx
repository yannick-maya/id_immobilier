import React, { useState, useEffect } from 'react';
import { useParams, Link } from 'react-router-dom';
import api from '../services/api';
import { useAuth } from '../context/AuthContext';

const BienDetail = () => {
  const { id } = useParams();
  const { user } = useAuth();
  const [annonce, setAnnonce] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [isFavori, setIsFavori] = useState(false);

  useEffect(() => {
    loadAnnonce();
    if (user) {
      checkFavori();
    }
  }, [id, user]);

  const loadAnnonce = async () => {
    try {
      setLoading(true);
      const response = await api.get(`/annonces/${id}`);
      setAnnonce(response.data);
    } catch (error) {
      console.error('Erreur lors du chargement de l\'annonce:', error);
      setError('Annonce non trouvée');
    } finally {
      setLoading(false);
    }
  };

  const checkFavori = async () => {
    try {
      const response = await api.get('/favoris');
      const favoris = response.data || [];
      setIsFavori(favoris.some(f => f.annonce_id === id));
    } catch (error) {
      console.error('Erreur lors de la vérification des favoris:', error);
    }
  };

  const toggleFavori = async () => {
    if (!user) return;

    try {
      if (isFavori) {
        await api.delete(`/favoris/${id}`);
      } else {
        await api.post(`/favoris/${id}`);
      }
      setIsFavori(!isFavori);
    } catch (error) {
      console.error('Erreur lors de la gestion des favoris:', error);
    }
  };

  const formatPrix = (prix) => {
    return new Intl.NumberFormat('fr-FR').format(prix) + ' FCFA';
  };

  const formatDate = (dateString) => {
    return new Date(dateString).toLocaleDateString('fr-FR', {
      year: 'numeric',
      month: 'long',
      day: 'numeric'
    });
  };

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-[#065A82]"></div>
      </div>
    );
  }

  if (error || !annonce) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <h2 className="text-2xl font-bold text-gray-800 mb-4">Annonce non trouvée</h2>
          <p className="text-gray-600 mb-6">{error}</p>
          <Link
            to="/recherche"
            className="bg-[#065A82] text-white px-6 py-3 rounded hover:bg-opacity-90 transition-colors"
          >
            Retour à la recherche
          </Link>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="max-w-4xl mx-auto px-4 py-8">
        {/* Navigation */}
        <div className="mb-6">
          <Link
            to="/recherche"
            className="text-[#065A82] hover:text-opacity-80 transition-colors"
          >
            ← Retour à la recherche
          </Link>
        </div>

        {/* Titre et actions */}
        <div className="bg-white rounded-lg shadow-md p-6 mb-6">
          <div className="flex justify-between items-start mb-4">
            <div>
              <h1 className="text-3xl font-bold text-[#065A82] mb-2">
                {annonce.titre}
              </h1>
              <div className="flex items-center space-x-4 text-gray-600">
                <span>📍 {annonce.zone}</span>
                <span>•</span>
                <span>{annonce.type_bien}</span>
                <span>•</span>
                <span>{annonce.type_offre}</span>
                <span>•</span>
                <span className={`px-2 py-1 rounded-full text-xs font-medium ${
                  annonce.source === 'coinafrique' ? 'bg-blue-100 text-blue-800' :
                  annonce.source === 'immoask' ? 'bg-green-100 text-green-800' :
                  annonce.source === 'facebook' ? 'bg-purple-100 text-purple-800' :
                  'bg-gray-100 text-gray-800'
                }`}>
                  {annonce.source}
                </span>
              </div>
            </div>

            {user && (
              <button
                onClick={toggleFavori}
                className={`text-2xl transition-colors ${
                  isFavori ? 'text-red-500' : 'text-gray-400 hover:text-red-500'
                }`}
                title={isFavori ? 'Retirer des favoris' : 'Ajouter aux favoris'}
              >
                {isFavori ? '❤️' : '🤍'}
              </button>
            )}
          </div>

          {/* Prix principal */}
          <div className="text-4xl font-bold text-[#065A82] mb-4">
            {formatPrix(annonce.prix)}
          </div>

          {/* Informations clés */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
            {annonce.surface_m2 && (
              <div className="text-center">
                <div className="text-2xl font-bold text-[#065A82]">{annonce.surface_m2}</div>
                <div className="text-sm text-gray-600">m²</div>
              </div>
            )}

            {annonce.pieces && (
              <div className="text-center">
                <div className="text-2xl font-bold text-[#065A82]">{annonce.pieces}</div>
                <div className="text-sm text-gray-600">pièces</div>
              </div>
            )}

            {annonce.Valeur_par_m2 && (
              <div className="text-center">
                <div className="text-2xl font-bold text-[#F59E0B]">{formatPrix(annonce.Valeur_par_m2)}</div>
                <div className="text-sm text-gray-600">prix/m²</div>
              </div>
            )}

            {annonce.date_annonce && (
              <div className="text-center">
                <div className="text-lg font-semibold text-gray-800">{formatDate(annonce.date_annonce)}</div>
                <div className="text-sm text-gray-600">publiée le</div>
              </div>
            )}
          </div>
        </div>

        {/* Détails supplémentaires */}
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* Informations détaillées */}
          <div className="lg:col-span-2 space-y-6">
            {/* Description */}
            {annonce.description && (
              <div className="bg-white rounded-lg shadow-md p-6">
                <h2 className="text-xl font-bold text-[#065A82] mb-4">Description</h2>
                <p className="text-gray-700 whitespace-pre-line">{annonce.description}</p>
              </div>
            )}

            {/* Caractéristiques */}
            <div className="bg-white rounded-lg shadow-md p-6">
              <h2 className="text-xl font-bold text-[#065A82] mb-4">Caractéristiques</h2>
              <div className="grid grid-cols-2 gap-4">
                {annonce.chambres && (
                  <div>
                    <span className="font-medium text-gray-700">Chambres:</span>
                    <span className="ml-2">{annonce.chambres}</span>
                  </div>
                )}

                {annonce.sdb && (
                  <div>
                    <span className="font-medium text-gray-700">Salles de bain:</span>
                    <span className="ml-2">{annonce.sdb}</span>
                  </div>
                )}

                {annonce.etage && (
                  <div>
                    <span className="font-medium text-gray-700">Étage:</span>
                    <span className="ml-2">{annonce.etage}</span>
                  </div>
                )}

                {annonce.meuble && (
                  <div>
                    <span className="font-medium text-gray-700">Meublé:</span>
                    <span className="ml-2">{annonce.meuble ? 'Oui' : 'Non'}</span>
                  </div>
                )}

                {annonce.annee_construction && (
                  <div>
                    <span className="font-medium text-gray-700">Année construction:</span>
                    <span className="ml-2">{annonce.annee_construction}</span>
                  </div>
                )}

                {annonce.etat && (
                  <div>
                    <span className="font-medium text-gray-700">État:</span>
                    <span className="ml-2">{annonce.etat}</span>
                  </div>
                )}
              </div>
            </div>

            {/* Localisation */}
            <div className="bg-white rounded-lg shadow-md p-6">
              <h2 className="text-xl font-bold text-[#065A82] mb-4">Localisation</h2>
              <div className="space-y-2">
                <p><span className="font-medium">Zone:</span> {annonce.zone}</p>
                {annonce.quartier && (
                  <p><span className="font-medium">Quartier:</span> {annonce.quartier}</p>
                )}
                {annonce.adresse && (
                  <p><span className="font-medium">Adresse:</span> {annonce.adresse}</p>
                )}
              </div>
            </div>
          </div>

          {/* Sidebar avec actions */}
          <div className="space-y-6">
            {/* Contact */}
            <div className="bg-white rounded-lg shadow-md p-6">
              <h2 className="text-xl font-bold text-[#065A82] mb-4">Contact</h2>
              <div className="space-y-3">
                {annonce.contact_nom && (
                  <p><span className="font-medium">Nom:</span> {annonce.contact_nom}</p>
                )}
                {annonce.contact_telephone && (
                  <p>
                    <span className="font-medium">Téléphone:</span>
                    <a
                      href={`tel:${annonce.contact_telephone}`}
                      className="text-[#065A82] hover:underline ml-2"
                    >
                      {annonce.contact_telephone}
                    </a>
                  </p>
                )}
                {annonce.contact_email && (
                  <p>
                    <span className="font-medium">Email:</span>
                    <a
                      href={`mailto:${annonce.contact_email}`}
                      className="text-[#065A82] hover:underline ml-2"
                    >
                      {annonce.contact_email}
                    </a>
                  </p>
                )}
              </div>
            </div>

            {/* Actions */}
            <div className="bg-white rounded-lg shadow-md p-6">
              <h2 className="text-xl font-bold text-[#065A82] mb-4">Actions</h2>
              <div className="space-y-3">
                <Link
                  to={`/simulateur?prix=${annonce.prix}&surface=${annonce.surface_m2 || ''}&zone=${encodeURIComponent(annonce.zone)}`}
                  className="block w-full bg-[#F59E0B] text-white text-center px-4 py-3 rounded hover:bg-opacity-90 transition-colors"
                >
                  Simuler un prêt
                </Link>

                <Link
                  to={`/indice?zone=${encodeURIComponent(annonce.zone)}`}
                  className="block w-full bg-[#065A82] text-white text-center px-4 py-3 rounded hover:bg-opacity-90 transition-colors"
                >
                  Voir les indices
                </Link>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default BienDetail;