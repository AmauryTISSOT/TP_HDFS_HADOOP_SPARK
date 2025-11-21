/**
 * Constante qui stocke toutes les fonctions qui font appel à notre API NodeJS pour manipuler la BDD
 */
const ProductApi = {
    /**
     * Fonction permetant d'appeler mon API pour obtenir la liste de tous les produits
     */
    getProduits: () => {
        return fetch("http://localhost:5043/produits").then((response) =>
            response.json()
        );
    },

    /**
     * Fonction permetant d'appeler mon API pour obtenir un produit selon son ID
     */
    getProduitById: (id) => {
        console.log("Id from getProduitById");
        return fetch(`http://localhost:5043/produit/${id}`).then((response) =>
            response.json()
        );
    },

    /**
     * Fonction permetant d'appeler mon API pour créer un nouveau produit
     */
    addProduit: (produit) => {
        return fetch("http://localhost:5043/produit", {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
            },
            body: JSON.stringify(produit),
        }).then((response) =>
            response.json().then((data) => ({
                status: response.status,
                data: data,
            }))
        );
    },

    /**
     * Fonction permetant d'appeler mon API pour supprimer un produit
     */
    deleteProduit: (id) => {
        return fetch(`http://localhost:5043/produit/${id}`, {
            method: "DELETE",
            headers: { "content-type": "application/json" },
        })
            .then(() => {
                console.log(`Produit ${id} supprimé`);
            })
            .catch((error) => {
                console.error(`Erreur lors de la suppression :`, error);
            });
    },

    /**
     * Fonction permetant d'appeler mon API pour mettre à jour un produit
     */
    updateProduit: (id, produit) => {
        return fetch(`http://localhost:5043/produit/${id}`, {
            method: "PUT",
            headers: {
                "Content-Type": "application/json",
            },
            body: JSON.stringify(produit),
        }).then((response) =>
            response.json().then((data) => ({
                status: response.status,
                data: data,
            }))
        );
    },

    /**
     * Fonction permetant d'appeler mon API pour obtenir un produit selon son ID dans le but de le mettre à jour
     * On sépare ici status et data
     */
    getProduitByIdToUpdate: (id) => {
        console.log("Id from getProduitById");
        return fetch(`http://localhost:5043/produit/${id}`).then((response) =>
            response.json().then((data) => ({
                status: response.status,
                data: data,
            }))
        );
    },

    /**
     * Ajout d'un produit au panier
     * @param {*} id
     * @returns http status code
     */
    addProduitToPanier: (id) => {
        console.log("Id from addProduitToPanier", id);
        return fetch(`http://localhost:5043/produit/${id}/panier`, {
            method: "PUT",
            headers: {
                "Content-Type": "application/json",
            },
        }).then((response) => response.json());
    },
};

export default ProductApi;
