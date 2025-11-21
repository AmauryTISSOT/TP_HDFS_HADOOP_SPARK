import styles from "./Panier.module.css";
import ItemPanier from "../../components/ItemPanier/ItemPanier";
import Bouton from "../../components/Bouton/Bouton";
import { usePanierContext } from "../../context/PanierContext";

const Panier = () => {
    // Récupère les id des articles présent dans le panier depuis le context ainsi que la fonction pour retirer un article
    const { panierItems, removeFromPanier } = usePanierContext();

    return (
        <div className={styles.container}>
            <h1 className={styles.title}>Mon panier</h1>
            {panierItems.length === 0 ? (
                <div className={styles.empty}>
                    <h1>Votre panier est vide ! 😮</h1>
                </div>
            ) : (
                <>
                    <div className={styles.itemsContainer}>
                        {panierItems.map((item) => (
                            <ItemPanier
                                key={item}
                                itemId={item}
                                deleteButtonFct={() => removeFromPanier(item)}
                            />
                        ))}
                    </div>
                    <Bouton
                        styles={styles.button}
                        label={"Procéder au paiement"}
                    />
                </>
            )}
        </div>
    );
};

export default Panier;
