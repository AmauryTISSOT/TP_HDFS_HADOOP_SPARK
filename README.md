# Etudiants :

| Nom                    | Spécialité    |
| ---------------------- | ------------- |
| Amaury TISSOT          | Front & Back  |
| Léa DRUFFIN            | Kafka |
| Hassan HOUSSEIN HOUMED | HDFS & Spark |

## Commandes à effectuer pour lancer le projet

Pour lancer le back Kafka HDFS et Spark :

```bash
docker compose up -d
```

Pour lancer le front :

```bash
cd front
npm install
npm run dev
```

## Credentials pour accéder à mongo - express :

identifiant : admin  
mot de passe : pass

# Schéma d'architecture global

# Journal technique

## Front

L'application utilise la stack MERN.
Le front de l'application utilise donc React ave Vite.

### Commandes disponibles

| Commande          | Description                                               |
| ----------------- | --------------------------------------------------------- |
| `npm run dev`     | Lance le serveur de développement Vite (hot-reload)       |
| `npm run build`   | Compile et optimise le projet pour la production (`dist`) |
| `npm run preview` | Lance un aperçu local de la version buildée               |
| `npm run lint`    | Vérifie la qualité du code avec ESLint                    |

### Dépendances

| Package                  | Version | Rôle principal                                   |
| ------------------------ | ------- | ------------------------------------------------ |
| **react**                | 19.0.0  | Bibliothèque principale React                    |
| **react-dom**            | 19.0.0  | Rendu des composants React dans le DOM           |
| **react-router-dom**     | 7.2.0   | Gestion du routing et navigation                 |
| **react-hook-form**      | 7.54.2  | Gestion performante et légère des formulaires    |
| **@hookform/resolvers**  | 4.1.0   | Intégration de schémas de validation (Yup, Zod…) |
| **yup**                  | 1.6.1   | Validation des données des formulaires           |
| **react-toastify**       | 11.0.3  | Affichage de notifications toast (succès/erreur) |
| **vite**                 | 6.1.0   | Outil de build ultra-rapide (dev)                |
| **@vitejs/plugin-react** | 4.3.4   | Support React + Fast Refresh pour Vite           |
| **eslint** + plugins     | 9.x     | Linting et respect des bonnes pratiques          |
| **@types/**              | 19.x    | Types TypeScript pour React et React-DOM (dev)   |

### Pages de l'application

| Chemin (path)  | Description de la page                                                        |
| -------------- | ----------------------------------------------------------------------------- |
| `/`            | Page d'accueil de l'application avec la liste des produits sous forme de card |
| `/detail/{id}` | Page affichant les détails d'un produit                                       |
| `/addProduit`  | Page affichant un formulaire permettant d'ajouter un produit                  |
| `/recherche`   | Page de recherche d'un produit                                                |
| `/recherche`   | Page affichant le panier de l'utilisateur                                     |

**Comment organisez-vous l’affichage des stocks pour que le client comprenne ce qu’il peut acheter ?**

L'affichage des stocks d'un produit s'effectue d'après les données provenant du backend.
Exemple d'affichage :  
![](https://i.imgur.com/mjwa0qi.png)

**Quels événements utilisateur (clic, ajout panier, validation) sont remontés au Back et potentiellement envoyés à Kafka ?**

Deux événements utilisateur déclenche un envoi de données vers Kafka :

-   lorsque l'utilisateur clique sur un article pour obtenir des détails (page `/detail/{id}`)
-   lorsque l'utilisateur ajoute un article dans son panier. Ici, le fichier `PanierContext.jsx` met en place un système de panier global côté front grâce au React Context.
    La fonction `addToPanier(id)` appelle l’API backend pour décrémenter le stock du produit qui génèrera l'envoi vers Kafka. Grâce au hook `usePanierContext`, n’importe quel composant de l’application peut accéder instantanément à la liste des produits dans le panier (`panierItems`) et aux fonctions d’ajout/suppression.

## Back

S'agissant d'une application avec la stack MERN, le backend de l'application utilise express.js comme framework backend et MongoDb comme base de données NoSQL.

### Dépendances du projet backend

| Package      | Version | Rôle principal                                                     |
| ------------ | ------- | ------------------------------------------------------------------ |
| **express**  | 4.21.2  | Framework web (API REST)                                           |
| **mongoose** | 8.10.1  | ODM MongoDB – gestion des modèles et requêtes                      |
| **joi**      | 17.13.3 | Validation des données entrantes depuis les formulaires front      |
| **kafkajs**  | 2.2.4   | Client Kafka – production d’événements (VIEW_PRODUCT, ADD_TO_CART) |
| **cors**     | 2.8.5   | Gestion du CORS pour le frontend                                   |
| **dotenv**   | 16.4.7  | Chargement des variables d’environnement (.env)                    |
| **nodemon**  | 3.1.9   | Serveur de développement                                           |

### Commandes disponibles

| Commande       | Description                                                               |
| -------------- | ------------------------------------------------------------------------- |
| `npm start`    | Lance le serveur en mode développement avec **nodemon**                   |
| `npm run seed` | Permet d'ajouter des données en BDD à l'aide du script (`data/SeedDb.js`) |

### Description des endpoints

Les routes de notre API figure dans le fichier `ProduitRoute.js`:  
![](https://i.imgur.com/UdLq9uZ.png)

Voici la description des différents endpoints de notre application :

| Méthode  | Endpoint                     | Description                                                              | Body requis ? | Remarques importantes                                   |
| -------- | ---------------------------- | ------------------------------------------------------------------------ | ------------- | ------------------------------------------------------- |
| `GET`    | `/produits`                  | Récupère la liste complète de tous les produits                          | Non           | Retourne un tableau de produits                         |
| `GET`    | `/produit/:id`               | Récupère un produit spécifique par son ID                                | Non           | Envoie un événement Kafka `VIEW_PRODUCT`                |
| `GET`    | `/produits/recherche?query=` | Recherche des produits par mot-clé dans le titre (insensible à la casse) | Non (query)   | Exemple : `/produits/recherche?query=chaussures`        |
| `POST`   | `/produit`                   | Crée un nouveau produit                                                  | Oui           | Validation Joi + champ `image` → tableau `images`       |
| `PUT`    | `/produit/:id`               | Met à jour un produit existant (titre, prix, slug, description, etc.)    | Oui           | Validation Joi obligatoire                              |
| `PUT`    | `/produit/:id/panier`        | Ajoute un produit au panier (décrémente le stock de 1)                   | Non           | Vérifie le stock + envoie événement Kafka `ADD_TO_CART` |
| `DELETE` | `/produit/:id`               | Supprime définitivement un produit par son ID                            | Non           | Retourne `204 No Content` si succès                     |

### Envoi d'événements Kafka

A l'aide du package `kafkajs`, les endpoints figurant dans le tableau ci-dessous transmette des évenements à Kafka

| Endpoint                  | Type d'événement | Topic       | Description                                                                                                              |
| ------------------------- | ---------------- | ----------- | ------------------------------------------------------------------------------------------------------------------------ |
| `GET /produit/:id`        | `VIEW_PRODUCT`   | `ecommerce` | Événement envoyé lorsqu’un utilisateur consulte la fiche détaillée d’un produit (inclut titre, prix, stock et timestamp) |
| `PUT /produit/:id/panier` | `ADD_TO_CART`    | `ecommerce` | Événement envoyé lorsqu’un produit est ajouté au panier (décrémente le stock de 1 et inclut le nouveau stock restant)    |

La configuration de la connexion avec Kafka s'effectue dans le fichier `/kafka/producer.js`:  
![](https://i.imgur.com/1Lb4FLp.png)

Ensuite, l'envoi de l'événement s'effectue directement depuis le controller (exemple avec `addProduitToPanier`) :  
![](https://i.imgur.com/wD67a3V.png)

**Comment garantissez-vous que les stocks restent cohérents quand plusieurs clients achètent en même temps (au moins conceptuellement) ?**

Il explique plusieurs façon de garantie la cohérence du stock d'un produit.
Il est ainsi possible de mettre en place :

-   Un verrouillage de la donnée dans MongoDb : MongoDb va venir vérouiller le document pendant l'exécution de l'opération. Autrement dit, si plusieurs requêtes arrivent en même temps pour diminuer le stock d'un même produit, MongoDb les traitera une par une grâce à une opération atomique
-   Mettre en place une fille d'attente avec un consumer unique : l’API ne modifie plus directement le stock, mais envoie immédiatement un événement ADD_TO_CART dans Kafka. Un seul consumer lit ces événements et procède à la diminution du stock. La gestion du stock étant limitée à un seul consomer, il n'y a plus de risque d'incohérence.

## Kafka

**Comment organisez-vous vos topics (par type d’événement, par domaine : orders, stock, catalogue) et pourquoi ?**

**Quelle clé de partition choisiriez-vous (id commande, id produit, autre) et quel est l’intérêt de ce choix ?**

**Comment votre organisation Kafka aide-t-elle à rejouer ou analyser les historiques (par ex. incident sur les commandes) ?**

## HDFS

**Proposez une arborescence HDFS pour ShopNow+ (ex : /ecommerce/brut/orders/, /ecommerce/curated/stocks/…)**

J'ai créé une arborescence en 3 couches pour organiser les données :

```
/user/spark/kafka_stream/
├── brut/
│   ├── events/
│   │   └── [événements Kafka bruts en Parquet]
│   └── [flux continu depuis Kafka]
├── curated/
│   ├── view_product/
│   ├── add_to_cart/
│   └── [données filtrées par type d'événement]
└── indicators/
    ├── top_viewed_products/
    ├── top_bought_products/
    ├── daily_revenue/
    ├── stock_alerts/
    ├── products_by_price/
    └── global_stats/
```

**Logique** :
- **`brut/`** : Données brutes depuis Kafka (Job 1 y écrit en continu)
- **`curated/`** : Données filtrées et enrichies par type d'événement
- **`indicators/`** : Résultats finaux des calculs (Job 2 y écrit les KPIs)

---

**Comment organiseriez-vous les données pour retrouver facilement : toutes les commandes d'un jour donné, l'historique des ventes d'un produit précis ?**

- **Toutes les commandes d'un jour donné** : Les données sont partitionnées par date dans `/brut/events/`. Je peux facilement accéder aux données d'une date spécifique.

- **Historique des ventes d'un produit précis** : Dans `/curated/add_to_cart/`, les événements contiennent `id_produit`. Avec Spark, un simple filtrage sur ce champ retourne toutes les ventes du produit. Le format Parquet (columnaire) optimise cette requête.

---

**Quels formats (JSON, CSV, Parquet…) utiliseriez-vous à quels endroits, et pourquoi ?**

| Chemin | Format | Justification |
| ------ | ------ | ------------- |
| `/brut/events/` | **Parquet** | Compression 5-10×, format columnaire, standard Big Data |
| `/curated/` | **Parquet** | Performance + compression optimale |
| `/indicators/` | **Parquet** | Résultats optimisés pour dashboards (lecture rapide) |

---


## Spark

**Quels indicateurs business mettriez-vous en place en priorité (TOP produits, CA par jour, taux de rupture de stock…) ?**

J'ai implémenté 6 KPIs métier :

1. **KPI 1: TOP 10 Produits les plus vus** - Consultations par produit
2. **KPI 2: TOP 10 Produits achetés + CA** - Rentabilité produits
3. **KPI 3: Chiffre d'affaires par jour** - Tendances de vente
4. **KPI 4: Alertes rupture de stock** - Produits à stock = 0
5. **KPI 5: Produits par gamme de prix** - Stratégie tarifaire
6. **KPI 6: Statistiques globales** - Santé plateforme

**Ce que j'ai trouvé** :
- 2574 événements traités
- 1844 consultations, 730 achats
- 346 620€ de CA
- Taux de conversion: **39,6%** (excellent!)
- 7 produits en rupture de stock

---

**Donner un exemple de règle métier que Spark peut calculer, par ex. : "produit en risque de rupture si…".**

**Exemple 1: Rupture de stock**
```
Si stock = 0 → Alerte immédiate
Produits détectés: Laptop Dell XPS, iPhone 15 Pro, AirPods Pro, Samsung Galaxy, iPad Air, Sony Headphones, Mechanical Keyboard
```

**Exemple 2: Taux de conversion produit**
```
Taux conversion = Achats / Consultations × 100%
Si taux > 39% → Produit très populaire (à promouvoir)
```

**Exemple 3: Pic de vente**
```
Si CA jour > CA moyen × 2 → Pic d'activité détecté
Observé: 29/11 avec 124 650€ (2× plus que les autres jours)
```

---

**Pour ce cas e-commerce, préférez-vous des traitements quasi temps réel ou par lots (jour, heure) ? Justifiez.**

**J'ai choisi : Batch quotidien (version 1)**

| Approche | Avantages | Inconvénients | Choix |
| -------- | --------- | ------------- | ----- |
| **Temps réel (Streaming)** | KPIs toujours à jour | Complexe, ressources | Futur |
| **Par lots (Batch)** | Simple, fiable | Délai quelques heures | ✅ Actuel |

**Justification** :
- Job 1 (Spark Streaming) récupère les événements Kafka et les sauvegarde en HDFS en continu
- Job 2 (Spark Batch) calcule une fois par jour tous les KPIs sur les données accumulées
- Les alertes rupture stock sont envoyées immédiatement via le backend (pas besoin de Spark temps réel)
- Suffisant pour piloter la plateforme

---

# Rapport de synthèse client

• flux expliqué en langage non technique,  
• rôle de chaque brique,  
• valeur pour ShopNow+,  
• place de votre rôle (full pipeline / spécialiste),  
• pistes d’évolution.
