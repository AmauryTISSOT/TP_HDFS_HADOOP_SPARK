/**
 * Composant Bouton qui affiche un bouton personnalisé.
 *
 * @param {Object} props - Les propriétés passées au composant.
 * @param {function} props.onClick - La fonction à appeler lors du clic sur le bouton.
 * @param {string} props.label - Le texte à afficher sur le bouton.
 * @param {string} props.styles - La classe CSS à appliquer au bouton pour son style.
 *
 * @returns {JSX.Element} Un élément `button` avec le texte et les styles fournis.
 */
const Bouton = ({ onClick, label, styles }) => {
    return (
        <button className={styles} onClick={onClick}>
            {label}
        </button>
    );
};

export default Bouton;
