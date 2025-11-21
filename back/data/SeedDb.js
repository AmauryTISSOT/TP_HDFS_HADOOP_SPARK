import Produit from "../model/Produit.js";
import datas from "./FakeProduits.js";
import connectDB from "../db/DbConnect.js";
import mongoose from "mongoose";

/**
 * Cette classe permet d'exécuter l'import de données présentes dans FakeProduits.js
 * On essaye d'importer avec le insertMany, si erreur on est catch avec l'erreur en question
 */
const seedDb = async () => {
    try {
        await connectDB();
        await Produit.deleteMany();
        const datasWithStock = datas.map((item) => ({
            ...item,
            stock: 5,
        }));
        await Produit.insertMany(datasWithStock);
        console.log("Données insérées avec succès");
        mongoose.disconnect();
    } catch (error) {
        console.error("Erreur lors de l'insertion des données :", error);
        mongoose.disconnect();
    }
};

seedDb();
