# Projet ETL Qualité de l'Air - Geodair

## Description du Projet

Ce projet a été réalisé dans le cadre du module Data Engineering. Il consiste en la conception et le déploiement d'un pipeline ETL (Extract, Transform, Load) automatisé.

L'objectif est de collecter quotidiennement des données de qualité de l'air, de les transformer pour les rendre exploitables, et de les charger dans un entrepôt de données pour analyse. Le flux permet d'alimenter un tableau de bord de visualisation pour le suivi des concentrations de polluants.

## Architecture Technique

Le pipeline repose sur une architecture Cloud utilisant les services suivants :

* **Orchestration :** Planification automatisée des tâches (Cloud Scheduler).
* **Extraction (Extract) :** Récupération des données brutes depuis l'API Geodair et stockage au format CSV dans un Data Lake (Cloud Storage - zone Raw).
* **Transformation (Transform) :** Nettoyage, typage, enrichissement des données et modélisation en schéma en étoile. Stockage des fichiers transformés (Cloud Storage - zone Transformed).
* **Chargement (Load) :** Insertion des données dans l'entrepôt de données (BigQuery).
* **Visualisation :** Restitution graphique des données via Looker Studio.

Les schémas détaillés de l'architecture et de la base de données se trouvent dans le dossier docs.

## Modélisation des Données

Les données sont organisées selon un modèle en étoile (Star Schema) comprenant :

* **Table de faits :** FACT_QUALITE_AIR (Mesures de concentration).
* **Dimensions :**
    * DIM_TEMPS (Axes temporels : heure, jour, mois).
    * DIM_SITE (Informations géographiques et typologie des stations).
    * DIM_POLLUANT (Détails sur les polluants : NO2, O3, PM10, etc.).
    * DIM_QUALITE (Référentiel des niveaux de risque).

Le détail des colonnes est disponible dans le Dictionnaire de Données (docs/DATA_DICTIONARY.csv).

## Prérequis

* Compte Google Cloud Platform actif.
* Python 3.x installé.
* Google Cloud SDK installé et configuré.

## Auteurs

Projet académique réalisé par [Mehdi BEN CHEIKH, Priscilia GBOSSAME, Paul Thurin KENFACK] et [Younes BELBOUAB].
