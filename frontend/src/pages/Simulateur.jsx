import React, { useState, useEffect } from 'react';
import { useSearchParams } from 'react-router-dom';

const Simulateur = () => {
  const [searchParams] = useSearchParams();

  // États pour les paramètres du prêt
  const [params, setParams] = useState({
    prix: searchParams.get('prix') || '',
    apport: '',
    taux: '5.5', // Taux d'intérêt annuel en %
    duree: '20', // Durée en années
    surface: searchParams.get('surface') || '',
    zone: searchParams.get('zone') || ''
  });

  // États pour les résultats
  const [resultats, setResultats] = useState(null);
  const [loading, setLoading] = useState(false);

  // Calculer automatiquement quand les paramètres changent
  useEffect(() => {
    if (params.prix && params.apport && params.taux && params.duree) {
      calculerPret();
    } else {
      setResultats(null);
    }
  }, [params]);

  const handleParamChange = (key, value) => {
    setParams(prev => ({ ...prev, [key]: value }));
  };

  const calculerPret = () => {
    setLoading(true);

    setTimeout(() => {
      const prix = parseFloat(params.prix);
      const apport = parseFloat(params.apport);
      const tauxAnnuel = parseFloat(params.taux) / 100;
      const dureeAnnees = parseFloat(params.duree);

      if (prix <= 0 || apport < 0 || apport > prix || tauxAnnuel <= 0 || dureeAnnees <= 0) {
        setResultats(null);
        setLoading(false);
        return;
      }

      // Calcul du montant emprunté
      const montantEmprunte = prix - apport;

      // Calcul du taux mensuel
      const tauxMensuel = tauxAnnuel / 12;

      // Nombre total de mensualités
      const nbMensualites = dureeAnnees * 12;

      // Calcul de la mensualité (formule du prêt amortissable)
      const mensualite = montantEmprunte * (tauxMensuel * Math.pow(1 + tauxMensuel, nbMensualites)) /
                        (Math.pow(1 + tauxMensuel, nbMensualites) - 1);

      // Calcul du coût total du crédit
      const coutTotalCredit = mensualite * nbMensualites;

      // Calcul des intérêts totaux
      const interetsTotaux = coutTotalCredit - montantEmprunte;

      // Calcul du taux d'endettement (approximatif, basé sur un salaire fictif)
      const salaireEstime = mensualite * 3; // Règle des 30% d'endettement
      const tauxEndettement = (mensualite / salaireEstime) * 100;

      setResultats({
        montantEmprunte,
        mensualite,
        coutTotalCredit,
        interetsTotaux,
        nbMensualites,
        salaireEstime,
        tauxEndettement
      });

      setLoading(false);
    }, 500); // Délai artificiel pour simuler le calcul
  };

  const formatMontant = (montant) => {
    return new Intl.NumberFormat('fr-FR', {
      style: 'currency',
      currency: 'XAF',
      minimumFractionDigits: 0,
      maximumFractionDigits: 0
    }).format(montant);
  };

  const formatPourcentage = (valeur) => {
    return `${valeur.toFixed(2)}%`;
  };

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="max-w-4xl mx-auto px-4 py-8">
        {/* Titre */}
        <div className="text-center mb-8">
          <h1 className="text-3xl font-bold text-[#065A82] mb-2">
            Simulateur de prêt immobilier
          </h1>
          <p className="text-gray-600">
            Calculez votre mensualité et le coût total de votre prêt
          </p>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
          {/* Formulaire de paramètres */}
          <div className="bg-white rounded-lg shadow-md p-6">
            <h2 className="text-xl font-bold text-[#065A82] mb-6">
              Paramètres du prêt
            </h2>

            <div className="space-y-4">
              {/* Prix du bien */}
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Prix du bien (FCFA) *
                </label>
                <input
                  type="number"
                  value={params.prix}
                  onChange={(e) => handleParamChange('prix', e.target.value)}
                  placeholder="Ex: 50000000"
                  className="w-full border border-gray-300 rounded-md px-3 py-2 focus:outline-none focus:ring-2 focus:ring-[#065A82]"
                  min="0"
                />
              </div>

              {/* Apport personnel */}
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Apport personnel (FCFA) *
                </label>
                <input
                  type="number"
                  value={params.apport}
                  onChange={(e) => handleParamChange('apport', e.target.value)}
                  placeholder="Ex: 10000000"
                  className="w-full border border-gray-300 rounded-md px-3 py-2 focus:outline-none focus:ring-2 focus:ring-[#065A82]"
                  min="0"
                />
              </div>

              {/* Taux d'intérêt */}
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Taux d'intérêt annuel (%) *
                </label>
                <input
                  type="number"
                  value={params.taux}
                  onChange={(e) => handleParamChange('taux', e.target.value)}
                  step="0.1"
                  placeholder="Ex: 5.5"
                  className="w-full border border-gray-300 rounded-md px-3 py-2 focus:outline-none focus:ring-2 focus:ring-[#065A82]"
                  min="0"
                  max="20"
                />
              </div>

              {/* Durée */}
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Durée du prêt (années) *
                </label>
                <select
                  value={params.duree}
                  onChange={(e) => handleParamChange('duree', e.target.value)}
                  className="w-full border border-gray-300 rounded-md px-3 py-2 focus:outline-none focus:ring-2 focus:ring-[#065A82]"
                >
                  <option value="5">5 ans</option>
                  <option value="10">10 ans</option>
                  <option value="15">15 ans</option>
                  <option value="20">20 ans</option>
                  <option value="25">25 ans</option>
                  <option value="30">30 ans</option>
                </select>
              </div>

              {/* Informations complémentaires */}
              <div className="pt-4 border-t border-gray-200">
                <h3 className="text-sm font-medium text-gray-700 mb-2">
                  Informations complémentaires
                </h3>

                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label className="block text-xs text-gray-600 mb-1">
                      Surface (m²)
                    </label>
                    <input
                      type="number"
                      value={params.surface}
                      onChange={(e) => handleParamChange('surface', e.target.value)}
                      placeholder="Surface"
                      className="w-full border border-gray-300 rounded px-2 py-1 text-sm"
                      min="0"
                    />
                  </div>

                  <div>
                    <label className="block text-xs text-gray-600 mb-1">
                      Zone
                    </label>
                    <input
                      type="text"
                      value={params.zone}
                      onChange={(e) => handleParamChange('zone', e.target.value)}
                      placeholder="Zone géographique"
                      className="w-full border border-gray-300 rounded px-2 py-1 text-sm"
                    />
                  </div>
                </div>
              </div>
            </div>
          </div>

          {/* Résultats */}
          <div className="bg-white rounded-lg shadow-md p-6">
            <h2 className="text-xl font-bold text-[#065A82] mb-6">
              Résultats de la simulation
            </h2>

            {loading ? (
              <div className="text-center py-8">
                <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-[#065A82] mx-auto"></div>
                <p className="mt-4 text-gray-600">Calcul en cours...</p>
              </div>
            ) : resultats ? (
              <div className="space-y-4">
                {/* Montant emprunté */}
                <div className="bg-blue-50 p-4 rounded-lg">
                  <div className="text-sm text-blue-600 font-medium">Montant emprunté</div>
                  <div className="text-2xl font-bold text-blue-800">
                    {formatMontant(resultats.montantEmprunte)}
                  </div>
                </div>

                {/* Mensualité */}
                <div className="bg-green-50 p-4 rounded-lg">
                  <div className="text-sm text-green-600 font-medium">Mensualité</div>
                  <div className="text-2xl font-bold text-green-800">
                    {formatMontant(resultats.mensualite)}
                  </div>
                  <div className="text-xs text-green-600 mt-1">
                    sur {resultats.nbMensualites} mois
                  </div>
                </div>

                {/* Coût total */}
                <div className="bg-orange-50 p-4 rounded-lg">
                  <div className="text-sm text-orange-600 font-medium">Coût total du crédit</div>
                  <div className="text-xl font-bold text-orange-800">
                    {formatMontant(resultats.coutTotalCredit)}
                  </div>
                </div>

                {/* Intérêts totaux */}
                <div className="bg-red-50 p-4 rounded-lg">
                  <div className="text-sm text-red-600 font-medium">Intérêts totaux</div>
                  <div className="text-xl font-bold text-red-800">
                    {formatMontant(resultats.interetsTotaux)}
                  </div>
                </div>

                {/* Taux d'endettement */}
                <div className="bg-purple-50 p-4 rounded-lg">
                  <div className="text-sm text-purple-600 font-medium">Taux d'endettement estimé</div>
                  <div className="text-xl font-bold text-purple-800">
                    {formatPourcentage(resultats.tauxEndettement)}
                  </div>
                  <div className="text-xs text-purple-600 mt-1">
                    Salaire estimé: {formatMontant(resultats.salaireEstime)}
                  </div>
                </div>

                {/* Informations importantes */}
                <div className="mt-6 p-4 bg-yellow-50 rounded-lg">
                  <h3 className="text-sm font-medium text-yellow-800 mb-2">
                    ℹ️ Informations importantes
                  </h3>
                  <ul className="text-xs text-yellow-700 space-y-1">
                    <li>• Cette simulation est indicative et non contractuelle</li>
                    <li>• Les taux peuvent varier selon votre profil</li>
                    <li>• Pensez aux frais annexes (notaire, assurance, etc.)</li>
                    <li>• Consultez un conseiller bancaire pour un devis précis</li>
                  </ul>
                </div>
              </div>
            ) : (
              <div className="text-center py-8 text-gray-500">
                <p>Remplissez tous les champs obligatoires pour voir la simulation</p>
              </div>
            )}
          </div>
        </div>

        {/* Conseils */}
        <div className="mt-8 bg-white rounded-lg shadow-md p-6">
          <h2 className="text-xl font-bold text-[#065A82] mb-4">
            Conseils pour votre prêt immobilier
          </h2>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div>
              <h3 className="font-semibold text-gray-800 mb-2">💰 Apport personnel</h3>
              <p className="text-sm text-gray-600">
                Un apport d'au moins 10-20% du prix du bien est recommandé pour réduire
                le montant emprunté et améliorer vos conditions d'emprunt.
              </p>
            </div>

            <div>
              <h3 className="font-semibold text-gray-800 mb-2">📊 Taux d'endettement</h3>
              <p className="text-sm text-gray-600">
                Le taux d'endettement ne devrait pas dépasser 35% de vos revenus.
                Les banques sont généralement plus strictes.
              </p>
            </div>

            <div>
              <h3 className="font-semibold text-gray-800 mb-2">⏰ Durée du prêt</h3>
              <p className="text-sm text-gray-600">
                Une durée plus longue réduit la mensualité mais augmente le coût total
                du crédit. Trouvez le bon équilibre selon votre situation.
              </p>
            </div>

            <div>
              <h3 className="font-semibold text-gray-800 mb-2">🏠 Assurance emprunteur</h3>
              <p className="text-sm text-gray-600">
                N'oubliez pas l'assurance emprunteur obligatoire qui représente
                environ 0.3-0.5% du montant emprunté par an.
              </p>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default Simulateur;