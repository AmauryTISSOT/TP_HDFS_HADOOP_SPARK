import React, { useEffect } from "react";
import { Link, useNavigate } from "react-router-dom";
import styles from "./SearchBar.module.css";

/**
 * Composant SearchBar qui permet de rechercher des produits.
 *
 * Ce composant contient une barre de recherche où l'utilisateur peut entrer un terme de recherche.
 * La recherche des produits se déclenche automatiquement à chaque modification du terme de recherche,
 * et les résultats sont affichés sous forme de liens cliquables.
 * Chaque produit peut être cliqué pour afficher les détails de ce produit.
 *
 * @returns {JSX.Element} Un élément contenant la barre de recherche et les résultats associés.
 */
const SearchBar = () => {
    const [query, setQuery] = React.useState("");
    const [produits, setProduits] = React.useState([]);
    const navigate = useNavigate();

    //Fonction pour récupérer les produits en fonction du terme de recherche
    const fetchProduits = async () => {
        try {
            const response = await fetch(
                `http://localhost:8081/produits/recherche?query=${query}`
            );
            if (!response.ok) {
                throw new Error("Erreur de réseau");
            }
            const data = await response.json();
            setProduits(data);
        } catch (error) {
            console.error(
                "Erreur lors de la récupération des produits:",
                error
            );
        }
    };

    // Utilisation de useEffect pour déclencher la recherche à chaque changement du terme
    useEffect(() => {
        fetchProduits();
    }, [query]);

    // Fonction pour gérer le clic sur un produit
    const handleClick = (id) => {
        navigate(`/detail/${id}`);
    };

    return (
        <section className={styles.mainContainer}>
            <input
                type="text"
                placeholder="Rechercher un produit"
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                className={styles.searchBar}
            />

            <div className={styles.resultContainer}>
                {produits.length > 0 ? (
                    produits.map((produit) => (
                        <Link
                            key={produit._id}
                            onClick={() => handleClick(produit._id)}
                            className={styles.resultItem}
                            to={`/detail/${produit._id}`}
                        >
                            <img
                                src={produit.images[0]}
                                alt={produit.title}
                                className={styles.resultImg}
                            />
                            <div className={styles.nameContainer}>
                                <h3 className={styles.resultName}>
                                    {produit.title}
                                </h3>
                            </div>
                            <p className={styles.resultPrice}>
                                {produit.price} €
                            </p>
                        </Link>
                    ))
                ) : (
                    <p>Aucun produit trouvé.</p>
                )}
            </div>
        </section>
    );
};

export default SearchBar;
