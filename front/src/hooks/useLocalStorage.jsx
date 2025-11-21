import { useState, useEffect } from "react";

/**
 * Hook personnalisé qui gère la lecture et l'écriture d'une valeur dans le stockage local du navigateur.
 *
 * Ce hook permet de récupérer une valeur depuis `localStorage` à l'initialisation et de la mettre à jour
 * lorsque la valeur change. Si la valeur n'existe pas dans `localStorage`, elle prend une valeur par défaut.
 *
 * @param {string} key - La clé sous laquelle la donnée sera stockée dans `localStorage`.
 * @param {any} initialValue - La valeur initiale à utiliser si aucune donnée n'est trouvée dans `localStorage`.
 * Elle peut être une valeur simple ou une fonction retournant une valeur.
 *
 * @returns {[any, function]} Un tableau contenant la valeur actuelle et la fonction pour la mettre à jour.
 */
const useLocalStorage = (key, initialValue) => {
    const [value, setValue] = useState(() => {
        // Récupération de la valeur depuis le localStorage
        const localStorageData = localStorage.getItem(key);
        if (localStorageData != null) {
            return JSON.parse(localStorageData); // Si la donnée existe, on la parse et on la retourne
        }
        // Si la valeur n'existe pas dans le localStorage, on utilise la valeur initiale
        if (typeof initialValue === "function") {
            return initialValue();
        } else {
            return initialValue;
        }
    });

    useEffect(() => {
        // Lorsque la valeur change, on met à jour le localStorage
        localStorage.setItem(key, JSON.stringify(value));
    }, [key, value]);

    return [value, setValue];
};

export default useLocalStorage;
