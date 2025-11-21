import { useForm } from "react-hook-form";
import ProductApi from "../../services/ProductApi.js";
import * as yup from "yup";
import { useNavigate, useParams } from "react-router-dom";
import { yupResolver } from "@hookform/resolvers/yup";
import { toast } from "react-toastify";
import "react-toastify/dist/ReactToastify.css";
import styles from "./AddProduct.module.css";
import { useEffect, useState } from "react";

/**
 * Schéma qui correspond à la déclaration des différents champs dans notre formulaire
 * Utilisé avec Yup pour la validation des champs du formulaire
 */
const schema = yup.object().shape({
    title: yup.string().required("Le titre est requis"),
    description: yup.string().required("La description est requise"),
    price: yup
        .number()
        .required("Le prix est requis")
        .positive("Le prix doit être positif"),
    images: yup.string(),
    slug: yup.string(),
    category: yup.string(),
});

/**
 * AddProduct est le composant qui correspond au formulaire d'ajout ou de modification d'un produit.
 * Il permet soit de créer un nouveau produit, soit de modifier un produit existant en fonction de
 * l'ID du produit présent dans l'URL.
 */
const AddProduct = () => {
    const { productId } = useParams();
    const navigate = useNavigate();
    const [loading, setLoading] = useState(false);

    // Utilisation du hook react-hook-form avec validation via yup
    const {
        register,
        handleSubmit,
        setValue,
        formState: { errors },
    } = useForm({ resolver: yupResolver(schema) });

    /**
     * useEffect qui permet de charger les données du produit si on est en mode modification.
     * Il se déclenche dès que productId change.
     */
    useEffect(() => {
        if (productId) {
            fetchProductData(productId);
        }
    }, [productId]);

    /**
     * Fonction qui va récupérer les différentes données du produit et les mapper dans notre formulaire
     * Cette fonction est appelée lors de la modification d'un produit existant.
     *
     * @param {string} id - L'ID du produit à récupérer.
     */
    const fetchProductData = async (id) => {
        try {
            const response = await ProductApi.getProduitByIdToUpdate(id);
            console.log(response);
            if (response.status === 200) {
                const productData = response.data;
                console.log("Produit récupéré :", productData);
                console.log(productData);

                setValue("title", productData.title || "");
                setValue("description", productData.description || "");
                setValue("slug", productData.slug || "");
                setValue("price", productData.price || "");
                setValue("images", productData.images[0] || "");
                setValue("category", productData.category || "");
            }
        } catch (error) {
            console.error("Erreur lors du chargement du produit", error);
        }
    };

    /**
     * Fonction qui va réaliser la soumission du formulaire et informer l'utilisateur du résultat
     * Cette fonction gère à la fois l'ajout et la mise à jour des produits.
     *
     * @param {Object} data - Les données du produit à soumettre, issues du formulaire.
     */
    const onSubmit = async (data) => {
        try {
            setLoading(true);
            let response;

            if (productId) {
                response = await ProductApi.updateProduit(productId, data);
                toast.success("✅ Produit mis à jour avec succès !");
            } else {
                response = await ProductApi.addProduit(data);
                toast.success("✅ Produit ajouté avec succès !");
            }

            if (response.status === 200 || response.status === 201) {
                navigate("/detail/" + response.data._id);
            } else {
                toast.error("❌ Erreur lors du traitement !");
            }
        } catch (error) {
            toast.error("❌ Une erreur est survenue !");
            console.error(error);
        } finally {
            setLoading(false);
        }
    };

    /**
     * Contenu graphique de la page AddProduct
     * Affiche le formulaire d'ajout ou de modification du produit en fonction du mode.
     *
     * @returns {JSX.Element} Le formulaire pour l'ajout ou la modification d'un produit.
     */
    return (
        <div className={styles.container}>
            <h1>{productId ? "Modifier le produit" : "Ajouter un produit"}</h1>

            <div className={styles.formContainer}>
                {loading ? (
                    <p>Chargement...</p>
                ) : (
                    <form onSubmit={handleSubmit(onSubmit)}>
                        <div>
                            <label>Titre</label>
                            <input {...register("title")} />
                            {errors.title && <p>{errors.title.message}</p>}
                        </div>

                        <div>
                            <label>Description</label>
                            <textarea {...register("description")} />
                            {errors.description && (
                                <p>{errors.description.message}</p>
                            )}
                        </div>

                        <div>
                            <label>Description réduite</label>
                            <input {...register("slug")} />
                            {errors.slug && <p>{errors.slug.message}</p>}
                        </div>

                        <div>
                            <label>Prix</label>
                            <input type="number" {...register("price")} />
                            {errors.price && <p>{errors.price.message}</p>}
                        </div>

                        <div>
                            <label>Image (lien imgur)</label>
                            <input {...register("images")} />
                            {errors.images && <p>{errors.images.message}</p>}
                        </div>

                        <div>
                            <label>Catégorie</label>
                            <input {...register("category")} />
                            {errors.category && (
                                <p>{errors.category.message}</p>
                            )}
                        </div>

                        <button type="submit" disabled={loading}>
                            {loading
                                ? "Enregistrement..."
                                : productId
                                ? "Mettre à jour"
                                : "Enregistrer"}
                        </button>
                    </form>
                )}
            </div>
        </div>
    );
};

export default AddProduct;
