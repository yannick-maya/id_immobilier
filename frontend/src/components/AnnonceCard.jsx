import React from 'react';
import { Link } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';

const AnnonceCard = ({ annonce, onToggleFavori }) => {
  const { user } = useAuth();

  const formatPrix = (prix) => {
    return new Intl.NumberFormat('fr-FR').format(prix) + ' FCFA';
  };

  const getSourceColor = (source) => {
    const colors = {
      'coinafrique': 'bg-blue-100 text-blue-800',
      'immoask': 'bg-green-100 text-green-800',
      'facebook': 'bg-purple-100 text-purple-800',
      'default': 'bg-gray-100 text-gray-800'
    };
    return colors[source] || colors.default;
  };

  return (
    <div className="bg-white rounded-lg shadow-md overflow-hidden hover:shadow-lg transition-shadow">
      <div className="p-6">
        <div className="flex justify-between items-start mb-4">
          <div>
            <h3 className="text-lg font-semibold text-[#065A82] mb-2 line-clamp-2">
              {annonce.titre}
            </h3>
            <p className="text-gray-600 text-sm mb-1">
              📍 {annonce.zone}
            </p>
            <p className="text-gray-600 text-sm">
              {annonce.type_bien} • {annonce.type_offre}
            </p>
          </div>
          <span className={`px-2 py-1 rounded-full text-xs font-medium ${getSourceColor(annonce.source)}`}>
            {annonce.source}
          </span>
        </div>

        <div className="space-y-2 mb-4">
          <div className="flex justify-between">
            <span className="text-gray-600">Prix:</span>
            <span className="font-bold text-[#065A82]">{formatPrix(annonce.prix)}</span>
          </div>
          {annonce.Valeur_par_m2 && (
            <div className="flex justify-between">
              <span className="text-gray-600">Prix/m²:</span>
              <span className="font-semibold text-[#F59E0B]">{formatPrix(annonce.Valeur_par_m2)}</span>
            </div>
          )}
          {annonce.surface_m2 && (
            <div className="flex justify-between">
              <span className="text-gray-600">Surface:</span>
              <span>{annonce.surface_m2} m²</span>
            </div>
          )}
          {annonce.pieces && (
            <div className="flex justify-between">
              <span className="text-gray-600">Pièces:</span>
              <span>{annonce.pieces}</span>
            </div>
          )}
        </div>

        <div className="flex justify-between items-center">
          <Link
            to={`/bien/${annonce.id}`}
            className="bg-[#065A82] text-white px-4 py-2 rounded hover:bg-opacity-90 transition-colors"
          >
            Voir détails
          </Link>

          {user && onToggleFavori && (
            <button
              onClick={() => onToggleFavori(annonce.id)}
              className="text-red-500 hover:text-red-700 transition-colors p-2"
              title="Ajouter aux favoris"
            >
              ❤️
            </button>
          )}
        </div>
      </div>
    </div>
  );
};

export default AnnonceCard;