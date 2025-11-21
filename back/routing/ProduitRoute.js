import express from "express";
import {
    getProduits,
    getProduitByID,
    newProduit,
    delProduit,
    majProduit,
    rechercherProduits,
    addProduitToPanier,
} from "../controller/ProduitController.js";

const router = express.Router();

/**
 * Toutes les routes nécessaires au fonctionnement de l'application
 */
router.get("/produits", getProduits);
router.get("/produit/:id", getProduitByID);
router.post("/produit", newProduit);
router.delete("/produit/:id", delProduit);
router.put("/produit/:id", majProduit);
router.put("/produit/:id/panier", addProduitToPanier);
router.get("/produits/recherche", rechercherProduits);

/**
 * Le router va nous permettre de faire un lien entre nos URL et nos fonctions ( lien front et back )
 */
export default router;
