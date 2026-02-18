-- ============================================================
-- Schema MySQL — ID Immobilier
-- Base : id_immobilier
-- ============================================================

CREATE DATABASE IF NOT EXISTS id_immobilier CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE id_immobilier;

-- Table 1 : Sources de données
CREATE TABLE IF NOT EXISTS source_donnees (
    id INT AUTO_INCREMENT PRIMARY KEY,
    nom VARCHAR(100) NOT NULL,          -- ex: ImmoAsk, Facebook, CoinAfrique
    url VARCHAR(255),
    date_collecte DATE,
    nombre_annonces INT DEFAULT 0
);

-- Table 2 : Zones géographiques
CREATE TABLE IF NOT EXISTS zone_geographique (
    id INT AUTO_INCREMENT PRIMARY KEY,
    nom VARCHAR(100) NOT NULL,          -- ex: adidogome, baguida, be
    commune VARCHAR(100),               -- ex: Lomé
    prefecture VARCHAR(100),            -- ex: GOLFE
    zone_admin VARCHAR(50)              -- ex: Zone 1, Zone 2
);

-- Table 3 : Biens immobiliers
CREATE TABLE IF NOT EXISTS bien_immobilier (
    id INT AUTO_INCREMENT PRIMARY KEY,
    type_bien VARCHAR(100),             -- Appartement, Terrain, Villa, Boutique
    type_offre VARCHAR(50),             -- VENTE, LOCATION
    surface_m2 DECIMAL(10,2),
    pieces INT,
    usage VARCHAR(50),                  -- Résidentiel, Commercial
    id_zone INT,
    FOREIGN KEY (id_zone) REFERENCES zone_geographique(id)
);

-- Table 4 : Annonces
CREATE TABLE IF NOT EXISTS annonce (
    id INT AUTO_INCREMENT PRIMARY KEY,
    titre VARCHAR(255),
    prix DECIMAL(15,2),
    prix_m2 DECIMAL(10,2),              -- Calculé : prix / surface_m2
    date_annonce DATE,
    id_bien INT,
    id_source INT,
    FOREIGN KEY (id_bien) REFERENCES bien_immobilier(id),
    FOREIGN KEY (id_source) REFERENCES source_donnees(id)
);

-- Table 5 : Statistiques par zone (résultats des calculs)
CREATE TABLE IF NOT EXISTS statistiques_zone (
    id INT AUTO_INCREMENT PRIMARY KEY,
    id_zone INT,
    type_bien VARCHAR(100),
    type_offre VARCHAR(50),
    periode VARCHAR(20),                -- ex: 2024-01, 2024-Q1
    prix_moyen_m2 DECIMAL(10,2),
    prix_median_m2 DECIMAL(10,2),
    prix_min DECIMAL(15,2),
    prix_max DECIMAL(15,2),
    nombre_annonces INT,
    ecart_valeur_venale DECIMAL(10,2),  -- Écart vs prix officiel
    FOREIGN KEY (id_zone) REFERENCES zone_geographique(id)
);

-- Table 6 : Indice immobilier
CREATE TABLE IF NOT EXISTS indice_immobilier (
    id INT AUTO_INCREMENT PRIMARY KEY,
    id_zone INT,
    type_bien VARCHAR(100),
    periode VARCHAR(20),
    prix_moyen_m2 DECIMAL(10,2),
    indice_valeur DECIMAL(10,4),        -- Base 100 = période référence
    tendance VARCHAR(20),               -- HAUSSE, BAISSE, STABLE
    FOREIGN KEY (id_zone) REFERENCES zone_geographique(id)
);

-- Table 7 : Valeurs vénales officielles
CREATE TABLE IF NOT EXISTS valeur_venale (
    id INT AUTO_INCREMENT PRIMARY KEY,
    id_zone INT,
    prix_m2_officiel DECIMAL(10,2),
    surface_m2 DECIMAL(10,2),
    valeur_totale DECIMAL(15,2),
    FOREIGN KEY (id_zone) REFERENCES zone_geographique(id)
);
