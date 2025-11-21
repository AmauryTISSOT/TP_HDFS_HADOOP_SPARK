import mongoose from "mongoose";
import Joi from "joi";

/**
 * Schema du type de donnée à recevoir
 */
const produitSchema = new mongoose.Schema({
    title: {
        type: String,
        required: true,
    },
    slug: {
        type: String,
    },
    description: {
        type: String,
        required: true,
    },
    price: {
        type: Number,
        required: true,
    },
    images: {
        type: [String],
    },
    creationAt: {
        type: Date,
    },
    updatedAt: {
        type: Date,
    },
    category: {
        type: String,
    },
    stock: {
        type: Number,
    },
});

const Produit = mongoose.model("Produit", produitSchema);

/**
 * Schema de validation pour le produit
 */
const produitValidation = Joi.object({
    title: Joi.string().required().messages({
        "string.empty": "Le title est obligatoire",
    }),
    description: Joi.string().required().messages({
        "string.empty": "La description est obligatoire",
    }),
    price: Joi.number().min(1).required().messages({
        "number.base": " Le prix doit être un nombre entier",
        "number.min": "Le prix doit être supérieur à 1 euro.",
    }),
    images: Joi.string().optional().allow(null, ""),
    slug: Joi.string().optional().allow(null, ""),
    category: Joi.string().optional().allow(null, ""),
});

export { Produit, produitValidation };
export default Produit;
