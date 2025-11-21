import { createContext, useContext } from "react";
import useLocalStorage from "../hooks/useLocalStorage";
import ProductApi from "../services/ProductApi.js";

const PanierContext = createContext({});

/**
 * Hook personnalisé qui permet d'accéder au contexte Panier.
 *
 * @returns {Object} L'objet contenant les valeurs du contexte Panier :
 * - addToPanier : Fonction pour ajouter un produit au panier.
 * - removeFromPanier : Fonction pour supprimer un produit du panier.
 * - panierItems : Tableau contenant les éléments actuellement dans le panier.
 */
export const usePanierContext = () => {
    return useContext(PanierContext);
};

/**
 * Fournisseur du contexte Panier. Ce composant permet de partager l'état du panier
 * à travers l'arbre de composants de l'application.
 *
 * @param {Object} props - Les propriétés passées au composant.
 * @param {React.ReactNode} props.children - Les enfants à envelopper dans le fournisseur du contexte.
 *
 * @returns {JSX.Element} Le fournisseur du contexte Panier avec les fonctions et l'état du panier.
 */
export const PanierProvider = ({ children }) => {
    const [panierItems, setPanierItems] = useLocalStorage("panier", []);

    /**
     * Fonction qui permet d'ajouter l'id d'un article au panier dans le contexte.
     *
     * @param {string|number} id - L'identifiant de l'article à ajouter au panier.
     */
    const addToPanier = async (id) => {
        try {
            await ProductApi.addProduitToPanier(id);
        } catch (error) {
            console.error("Erreur lors de l'ajout du produit au panier", error);
        }
        setPanierItems((item) => {
            if (id !== undefined) {
                return [...item, id];
            }
        });
    };

    /**
     * Fonction qui permet de supprimer l'id d'un article du panier dans le contexte.
     *
     * @param {string|number} id - L'identifiant de l'article à supprimer du panier.
     */
    const removeFromPanier = (id) => {
        setPanierItems((item) => {
            return item.filter((item) => item !== id);
        });
    };

    return (
        <PanierContext.Provider
            value={{ addToPanier, removeFromPanier, panierItems }}
        >
            {children}
        </PanierContext.Provider>
    );
};
