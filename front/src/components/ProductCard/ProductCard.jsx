import styles from "./ProductCard.module.css";
import React, { useEffect } from "react";
import postApi from "../../services/PostApi.js";
import Bouton from "../Bouton/Bouton.jsx";
import { Link } from "react-router-dom";
import { usePanierContext } from "../../context/PanierContext.jsx";

/**
 * Composant ProductCard qui affiche une carte de produit avec une option d'ajout au panier.
 *
 * Ce composant récupère la liste des produits depuis l'API et permet à l'utilisateur de voir les détails du produit
 * ainsi que d'ajouter le produit au panier via un bouton.
 *
 * @returns {JSX.Element} Un élément contenant une grille de cartes de produits avec une image, un titre, un prix,
 * et un bouton pour ajouter le produit au panier.
 */
const ProductCard = () => {
    const [product, setProduct] = React.useState([]);

    const { addToPanier } = usePanierContext();

    useEffect(() => {
        postApi.getProduits().then((response) => {
            setProduct(response);
        });
    }, []);

    return (
        <section className={styles.grid}>
            {product.map((produit) => (
                <div key={produit._id} className={styles.item}>
                    <Link to={`/detail/${produit._id}`} className={styles.link}>
                        <img
                            src={produit.images[0]}
                            alt={produit.title}
                            className={styles.image}
                        />
                        <div className={styles.cardContent}>
                            <p>{produit.title}</p>
                            <p>{produit.price} €</p>
                        </div>
                    </Link>
                    <Bouton
                        styles={styles.button}
                        label={"Ajouter au panier"}
                        onClick={() => addToPanier(produit._id)}
                    />
                </div>
            ))}
        </section>
    );
};

export default ProductCard;
