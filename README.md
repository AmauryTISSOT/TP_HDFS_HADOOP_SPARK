# Etudiants :

| Nom                    | Spécialité    |
| ---------------------- | ------------- |
| Amaury TISSOT          | Front & Back  |
| Léa DRUFFIN            | Kafka & Spark |
| Hassan HOUSSEIN HOUMED | HDFS          |

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

**Quelles pages principales proposez-vous ?**

**Comment organisez-vous l’affichage des stocks pour que le client comprenne ce qu’il peut acheter ?**

**Quels événements utilisateur (clic, ajout panier, validation) sont remontés au Back et potentiellement envoyés à Kafka ?**

## Back

S'agissant d'une application avec la stack MERN, le backend de l'application utilise express.js comme framework backend et MongoDb comme base de données NoSQL.

### Dépendances du projet backend

| Package      | Version  | Rôle principal                                                     |
| ------------ | -------- | ------------------------------------------------------------------ |
| **express**  | ^4.21.2  | Framework web (API REST)                                           |
| **mongoose** | ^8.10.1  | ODM MongoDB – gestion des modèles et requêtes                      |
| **joi**      | ^17.13.3 | Validation des données entrantes depuis les formulaires front      |
| **kafkajs**  | ^2.2.4   | Client Kafka – production d’événements (VIEW_PRODUCT, ADD_TO_CART) |
| **cors**     | ^2.8.5   | Gestion du CORS pour le frontend                                   |
| **dotenv**   | ^16.4.7  | Chargement des variables d’environnement (.env)                    |
| **nodemon**  | ^3.1.9   | Serveur de développement                                           |

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

**Comment organiseriez-vous les données pour retrouver facilement : toutes les commandes d’un jour donné, l’historique des ventes d’un produit précis ?**

**Quels formats (JSON, CSV, Parquet…) utiliseriez-vous à quels endroits, et pourquoi ?**

## Spark

**Quels indicateurs business mettriez-vous en place en priorité (TOP produits, CA par jour, taux de rupture de stock…) ?**

**Donner un exemple de règle métier que Spark peut calculer, par ex. : “produit en risque de rupture si…”.**

**Pour ce cas e-commerce, préférez-vous des traitements quasi temps réel ou par lots (jour, heure) ?**

# Rapport de synthèse client

• flux expliqué en langage non technique,  
• rôle de chaque brique,  
• valeur pour ShopNow+,  
• place de votre rôle (full pipeline / spécialiste),  
• pistes d’évolution.
