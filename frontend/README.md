# ID Immobilier - Frontend React

Interface utilisateur moderne pour la plateforme immobilière ID Immobilier, développée avec React et Tailwind CSS.

## 🚀 Fonctionnalités

### Pages Utilisateur
- **Accueil** : Présentation de la plateforme avec KPI et tendances du marché
- **Recherche** : Moteur de recherche avancé avec filtres multiples
- **Détail d'annonce** : Vue détaillée des biens avec simulateur de prêt intégré
- **Favoris** : Gestion des annonces sauvegardées
- **Simulateur de prêt** : Calculateur de mensualités et coût total du crédit
- **Indices immobiliers** : Statistiques et évolution des prix par zone
- **Tableau de bord** : Vue d'ensemble personnalisée pour l'utilisateur
- **Profil** : Gestion des informations personnelles et mot de passe

### Fonctionnalités Techniques
- 🔐 Authentification JWT avec gestion d'état
- 🎨 Interface responsive avec Tailwind CSS
- 🔄 Appels API avec Axios et intercepteur JWT
- 🧭 Routing avec React Router
- 📱 Design mobile-first
- ⚡ Performance optimisée

## 🛠️ Technologies Utilisées

- **React 18** : Framework JavaScript
- **React Router** : Routing côté client
- **Tailwind CSS** : Framework CSS utilitaire
- **Axios** : Client HTTP pour les appels API
- **Context API** : Gestion d'état globale
- **JWT** : Authentification token-based

## 📁 Structure du Projet

```
frontend/
├── public/
│   ├── index.html
│   └── ...
├── src/
│   ├── components/
│   │   ├── Navbar.jsx          # Navigation principale
│   │   └── AnnonceCard.jsx     # Carte d'annonce réutilisable
│   ├── context/
│   │   └── AuthContext.jsx     # Contexte d'authentification
│   ├── pages/
│   │   ├── Home.jsx            # Page d'accueil
│   │   ├── Login.jsx           # Connexion
│   │   ├── Register.jsx        # Inscription
│   │   ├── Dashboard.jsx       # Tableau de bord
│   │   ├── Recherche.jsx       # Recherche d'annonces
│   │   ├── BienDetail.jsx      # Détail d'une annonce
│   │   ├── Favoris.jsx         # Favoris utilisateur
│   │   ├── Indice.jsx          # Indices immobiliers
│   │   ├── Simulateur.jsx      # Simulateur de prêt
│   │   └── Profil.jsx          # Profil utilisateur
│   ├── services/
│   │   └── api.js              # Configuration Axios
│   ├── App.jsx                 # Application principale
│   ├── index.js                # Point d'entrée
│   └── ...
├── package.json
└── README.md
```

## 🚀 Installation et Démarrage

### Prérequis
- Node.js 16+
- npm ou yarn
- API backend en cours d'exécution

### Installation
```bash
cd frontend
npm install
```

### Configuration
Créer un fichier `.env` dans le dossier frontend :
```env
REACT_APP_API_URL=http://localhost:8000
```

### Démarrage
```bash
npm start
```

L'application sera accessible sur `http://localhost:3000`

## 🔧 Scripts Disponibles

- `npm start` : Démarre le serveur de développement
- `npm run build` : Construit l'application pour la production
- `npm test` : Lance les tests
- `npm run eject` : Éjecte de Create React App (irréversible)

## 🔐 Authentification

L'application utilise un système d'authentification basé sur JWT :

- **Login/Register** : Pages d'authentification
- **AuthContext** : Gestion de l'état d'authentification
- **Intercepteur Axios** : Injection automatique du token JWT
- **Routes protégées** : Accès conditionnel selon l'état de connexion

## 🎨 Design System

### Couleurs Principales
- **Primaire** : `#065A82` (Bleu foncé)
- **Secondaire** : `#F59E0B` (Orange)
- **Accent** : `#10B981` (Vert)
- **Danger** : `#EF4444` (Rouge)

### Composants Réutilisables
- **Navbar** : Navigation responsive
- **AnnonceCard** : Affichage standardisé des annonces
- **Formulaires** : Validation et gestion d'erreurs
- **Modales** : Confirmations et messages

## 📱 Responsive Design

L'application est entièrement responsive :
- **Mobile** : Interface adaptée aux petits écrans
- **Tablette** : Layout optimisé pour les écrans moyens
- **Desktop** : Interface complète avec toutes les fonctionnalités

## 🔄 Intégration API

### Endpoints Utilisés
- `GET /annonces` : Recherche d'annonces
- `GET /annonces/:id` : Détail d'une annonce
- `GET /statistiques/*` : Données statistiques
- `GET /indice` : Indices immobiliers
- `POST /auth/login` : Connexion
- `POST /auth/register` : Inscription
- `GET /favoris` : Favoris utilisateur
- `POST /favoris/:id` : Ajouter aux favoris

### Gestion d'État
- **AuthContext** : État global d'authentification
- **Local Storage** : Persistance du token JWT
- **State local** : Gestion des formulaires et données temporaires

## 🚀 Déploiement

### Build de Production
```bash
npm run build
```

### Serveur Statique
Le dossier `build` peut être servi par n'importe quel serveur web.

### Configuration Nginx
```nginx
server {
    listen 80;
    server_name votre-domaine.com;
    root /path/to/build;
    index index.html;

    location / {
        try_files $uri $uri/ /index.html;
    }

    location /api {
        proxy_pass http://localhost:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

## 🧪 Tests

```bash
npm test
```

## 📈 Performance

### Optimisations
- **Code Splitting** : Chargement paresseux des composants
- **Images optimisées** : Format WebP et lazy loading
- **Bundle analysé** : Taille optimisée du bundle
- **Caching** : Cache des requêtes API

## 🤝 Contribution

1. Fork le projet
2. Créer une branche feature (`git checkout -b feature/AmazingFeature`)
3. Commit les changements (`git commit -m 'Add some AmazingFeature'`)
4. Push vers la branche (`git push origin feature/AmazingFeature`)
5. Ouvrir une Pull Request

## 📝 Licence

Ce projet est sous licence MIT - voir le fichier [LICENSE](LICENSE) pour plus de détails.

## 📞 Support

Pour toute question ou problème :
- Créer une issue sur GitHub
- Contacter l'équipe de développement

---

Développé avec ❤️ pour la plateforme ID Immobilier