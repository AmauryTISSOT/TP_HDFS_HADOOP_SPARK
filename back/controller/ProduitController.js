import { Produit, produitValidation } from "../model/Produit.js";
import { producer } from "../kafka/producer.js";

/**
 * Controller pour récupérer la liste de tous les produits.
 * Cette fonction interroge la base de données pour obtenir tous les produits
 * et renvoie la liste en réponse.
 */
export const getProduits = async (req, res) => {
    try {
        // Récupération de tous les produits dans la base de données
        const produits = await Produit.find();
        res.status(200).send(produits); // Envoi des produits au client avec un statut 200 (OK)
    } catch (error) {
        console.error(error); // Affiche l'erreur dans la console
        res.status(500).json({ message: "Une erreur est survenue" }); // Erreur serveur
    }
};

/**
 * Controller pour récupérer un produit spécifique grâce à son ID dans l'URL.
 * Si le produit existe, il est renvoyé dans la réponse, sinon un message d'erreur est retourné.
 */
export const getProduitByID = async (req, res) => {
    try {
        const produit = await Produit.findById(req.params.id);

        if (!produit) {
            return res.status(404).json({ message: "Produit non trouvé" });
        }

        await producer.send({
            topic: "ecommerce",
            messages: [
                {
                    key: produit._id.toString(),
                    value: JSON.stringify({
                        type: "VIEW_PRODUCT",
                        produitId: produit._id,
                        title: produit.title,
                        price: produit.price,
                        timestamp: Date.now(),
                        stock: produit.stock,
                    }),
                },
            ],
        });

        console.log(`Consultation du produit ${produit._id} envoyée à Kafka`);

        res.status(200).json(produit);
    } catch (error) {
        console.error(error);
        res.status(500).json({ message: "Une erreur est survenue" });
    }
};

/**
 * Controller pour créer un nouveau produit.
 * Cette fonction valide les données envoyées, crée un produit et le sauvegarde dans la base de données.
 * Si la validation échoue, un message d'erreur est renvoyé.
 */
export const newProduit = async (req, res) => {
    try {
        // Validation des données du produit avec un schéma de validation
        const { error, value } = produitValidation.validate(req.body);

        // Si la validation échoue, renvoie un message d'erreur avec un statut 400
        if (error != undefined) {
            return res.status(400).json({ message: error.details[0].message });
        }

        // Création d'une nouvelle instance de Produit avec les données reçues
        const nouveauProduit = new Produit(req.body);
        nouveauProduit.creationAt = new Date(); // Ajout de la date de création

        // Si une image est fournie, on l'ajoute dans le tableau des images
        if (req.body.image && nouveauProduit) {
            nouveauProduit.images = [req.body.image];
        }

        // Sauvegarde du produit dans la base de données
        const produitAdd = await nouveauProduit.save();
        res.status(201).json(produitAdd); // Renvoie le produit ajouté avec un statut 201 (créé)
    } catch (error) {
        console.error(error); // Affiche l'erreur dans la console
        res.status(500).json({
            message: "Une erreur est survenue lors de la sauvegarde", // Erreur serveur
        });
    }
};

/**
 * Controller pour supprimer un produit existant.
 * Cette fonction cherche le produit par son ID, puis le supprime si trouvé.
 * Si le produit n'est pas trouvé, un message d'erreur 404 est renvoyé.
 */
export const delProduit = async (req, res) => {
    try {
        // Recherche du produit à supprimer par son ID
        const produitSupp = await Produit.findById(req.params.id);
        console.log(produitSupp); // Affiche le produit trouvé dans la console

        // Si le produit n'est pas trouvé, renvoie une erreur 404
        if (!produitSupp) {
            return res.status(404).json({ message: "Produit non trouvé" });
        }

        // Suppression du produit
        await produitSupp.deleteOne();
        res.status(204).end(); // Réponse vide avec un statut 204 (suppression réussie)
    } catch (error) {
        console.error(error); // Affiche l'erreur dans la console
        res.status(500).json({
            message: "Une erreur est survenue lors de la suppression", // Erreur serveur
        });
    }
};

/**
 * Controller pour mettre à jour un produit existant.
 * Cette fonction valide les nouvelles données et met à jour le produit dans la base de données.
 * Si le produit n'est pas trouvé, un message d'erreur 404 est renvoyé.
 */
export const majProduit = async (req, res) => {
    try {
        // Validation des données du produit à mettre à jour
        const { error, value } = produitValidation.validate(req.body);
        if (error) {
            return res.status(400).json({ message: error.details[0].message }); // Erreur de validation
        }

        // Mise à jour du produit par son ID
        const produitMaj = await Produit.findByIdAndUpdate(
            req.params.id,
            {
                title: req.body.title,
                price: req.body.price,
                slug: req.body.slug,
                description: req.body.description,
                category: req.body.category,
                images: [req.body.image],
            },
            { new: true } // Retourne le produit mis à jour
        );

        // Si le produit n'est pas trouvé, renvoie une erreur 404
        if (!produitMaj) {
            return res.status(404).json({ message: "Produit non trouvé" });
        }

        // Renvoie le produit mis à jour
        res.status(200).json(produitMaj);
    } catch (error) {
        console.error(error); // Affiche l'erreur dans la console
        res.status(500).json({
            message: "Une erreur est survenue lors de la mise à jour", // Erreur serveur
        });
    }
};

/**
 * Controller pour rechercher des produits par nom ou description.
 * Cette fonction permet de filtrer les produits en fonction d'un texte de recherche passé en paramètre.
 */
export const rechercherProduits = async (req, res) => {
    try {
        // Récupère le terme de recherche depuis la requête
        const { query } = req.query;

        // Recherche des produits dont le titre correspond au terme de recherche (insensible à la casse)
        const produits = await Produit.find({
            title: { $regex: query, $options: "i" }, // Utilisation d'une expression régulière pour la recherche
        });

        // Renvoie les produits trouvés
        res.status(200).json(produits);
    } catch (error) {
        console.error("Erreur lors de la recherche de produits:", error); // Affiche l'erreur dans la console
        res.status(500).json({
            message: "Erreur lors de la recherche de produits", // Erreur serveur
        });
    }
};

export const addProduitToPanier = async (req, res) => {
    try {
        const produit = await Produit.findById(req.params.id);

        if (!produit) {
            return res.status(404).json({ message: "Produit non trouvé" });
        }

        // Vérification du stock disponible
        if (produit.stock <= 0) {
            return res.status(400).json({ message: "Stock insuffisant" });
        }

        // Décrémenter le stock
        produit.stock -= 1;
        await produit.save();

        // Envoi de l'événement dans Kafka
        await producer.send({
            topic: "ecommerce",
            messages: [
                {
                    key: req.params.id,
                    value: JSON.stringify({
                        type: "ADD_TO_CART",
                        produitId: produit._id,
                        title: produit.title,
                        price: produit.price,
                        newStock: produit.stock,
                        timestamp: Date.now(),
                    }),
                },
            ],
        });

        console.log(
            `Produit ${produit._id} ajouté au panier, stock décrémenté et envoyé à Kafka`
        );

        res.status(200).json({
            message: "Produit ajouté au panier",
            produit,
        });
    } catch (error) {
        console.error(error);
        res.status(500).json({ message: "Une erreur est survenue" });
    }
};
