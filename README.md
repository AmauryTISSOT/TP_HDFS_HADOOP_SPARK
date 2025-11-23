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

**Quels sont, selon vous, les endpoints / services minimum pour gérer** :

-   **les produits,**
-   **les stocks,**
-   **les commandes**

**À quel moment le Back envoie-t-il des événements vers Kafka ?**

**Comment garantissez-vous que les stocks restent cohérents quand plusieurs clients achètent en même temps (au moins conceptuellement) ?**

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
