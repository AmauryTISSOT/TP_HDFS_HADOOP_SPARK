import { useEffect, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import ProductApi from "../../services/ProductApi.js";
import styles from "./ProductDetail.module.css";
import Bouton from "../../components/Bouton/Bouton.jsx";
import { usePanierContext } from "../../context/PanierContext.jsx";

/**
 * ProductDetail est le composant qui affiche les détails d'un produit.
 * Il permet d'ajouter un produit au panier, de le supprimer ou de le modifier.
 */
const ProductDetail = () => {
    const [productDetail, setProductDetail] = useState({});
    const { id } = useParams();
    const navigate = useNavigate();

    /**
     * useEffect pour récupérer les informations du produit au chargement du composant.
     * La récupération se fait via l'API `getProduitById` et met à jour `productDetail`.
     */
    useEffect(() => {
        ProductApi.getProduitById(id).then((response) =>
            setProductDetail(response)
        );
    }, [id]);

    const { addToPanier } = usePanierContext();

    /**
     * Fonction qui permet de revenir à la page d'accueil
     */
    const handleNavigate = () => {
        navigate("/");
    };

    /**
     * Fonction pour supprimer un produit. Elle appelle l'API `deleteProduit`
     * et après suppression, redirige l'utilisateur vers la page d'accueil.
     */
    const deleteProduct = (id) => {
        PostApi.deleteProduit(id).then((response) => {
            alert(`Product deleted successfully` + response);
        });
        handleNavigate();
    };

    /**
     * Fonction pour rediriger vers la page de modification d'un produit.
     */
    const updateProduct = (id) => {
        navigate(`/addProduit/${id}`);
    };

    return (
        <div className={styles.container}>
            {productDetail.images && productDetail.images.length > 0 && (
                <img src={productDetail.images[0]} className={styles.image} />
            )}
            <div className={styles.textContainer}>
                <div className={styles.headContainer}>
                    <h2 className={styles.category}>
                        {productDetail?.category || "Categorie non disponible"}
                    </h2>
                    <h1>{productDetail?.title || "Titre non disponible"}</h1>
                </div>
                <p>{productDetail?.price || "Prix non disponible"} €</p>
                <p className={styles.description}>
                    {productDetail?.description || "Description non disponible"}
                </p>
                <p className={styles.description}>
                    Stock :{" "}
                    <b>{productDetail?.stock || "Stock non disponible"}</b>
                </p>
                <div className={styles.buttonContainer}>
                    <Bouton
                        styles={styles.buttonAddPanier}
                        label={"Ajouter au panier"}
                        onClick={async () => await addToPanier(id)}
                    />
                    <Bouton
                        styles={styles.buttonDeleteArticle}
                        label={"Supprimer l'article"}
                        onClick={() => deleteProduct(id)}
                    />
                    <Bouton
                        styles={styles.buttonDeleteArticle}
                        label={"Modifier l'article"}
                        onClick={() => updateProduct(id)}
                    />
                </div>
            </div>
        </div>
    );
};

export default ProductDetail;
