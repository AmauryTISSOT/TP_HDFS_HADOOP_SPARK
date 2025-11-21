import { useState, useEffect } from "react";
import Bouton from "../Bouton/Bouton";
import ProductApi from "../../services/ProductApi";
import styles from "./ItemPanier.module.css";

/**
 * Composant ItemPanier qui affiche un article dans le panier.
 *
 * @param {Object} props - Les propriétés passées au composant.
 * @param {string} props.itemId - L'identifiant de l'article à afficher.
 * @param {function} props.deleteButtonFct - La fonction à appeler lorsqu'on clique sur le bouton de suppression.
 *
 * @returns {JSX.Element} Un élément contenant les informations d'un produit dans le panier avec une option de suppression.
 */
const ItemPanier = ({ itemId, deleteButtonFct }) => {
    const [item, setItems] = useState({});

    // On récupère la donnée à l'aide de l'item id et on la stock dans un useState
    useEffect(() => {
        ProductApi.getProduitById(itemId).then((response) =>
            setItems(response)
        );
    }, [itemId]);

    return (
        <div className={styles.container}>
            <div className={styles.content}>
                <img
                    src={item?.images?.[0]}
                    alt={item?.title}
                    className={styles.image}
                />
                <h3 className={styles.name}>{item?.title}</h3>
                <p className={styles.price}>{item?.price} €</p>
                <p>
                    Stock : <b>{item?.stock}</b>
                </p>
                <div className={styles.buttonContainer}>
                    <Bouton
                        label={"X"}
                        onClick={deleteButtonFct}
                        styles={styles.removeButton}
                    />
                </div>
            </div>
        </div>
    );
};

export default ItemPanier;
