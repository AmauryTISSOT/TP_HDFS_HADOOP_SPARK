import { Link } from "react-router-dom";
import styles from "./Header.module.css";

const Header = () => {
    return (
        <header className={styles.header}>
            <h1>
                <Link to="/" className={styles.logo}>
                    ShopNow+
                </Link>
            </h1>
            <nav>
                <ul className={styles.navList}>
                    <li>
                        <Link to="/">Accueil</Link>
                    </li>
                    <li>
                        <Link to="/addProduit">Ajouter un article</Link>
                    </li>
                    <li>
                        <Link to="/recherche">Recherche</Link>
                    </li>
                    <li>
                        <Link to="/panier">Panier</Link>
                    </li>
                </ul>
            </nav>
        </header>
    );
};

export default Header;
