import { BrowserRouter, Route, Routes } from "react-router-dom";
import "./App.css";
import Layout from "./layout/Layout";
import Home from "./pages/Home/Home";
import ProductDetail from "./pages/ProductDetail/ProductDetail";
import Panier from "./pages/Panier/Panier";
import AddProduct from "./pages/AddProduct/AddProduct.jsx";
import { ToastContainer } from "react-toastify";
import { PanierProvider } from "./context/PanierContext.jsx";
import SearchBar from "./components/SearchBar/SearchBar.jsx";

function App() {
    return (
        <BrowserRouter>
            <PanierProvider>
                <Routes>
                    <Route path="/" element={<Layout />}>
                        <Route index element={<Home />} />
                        <Route path="/detail/:id" element={<ProductDetail />} />
                        <Route path="/panier" element={<Panier />} />
                        <Route path="/addProduit" element={<AddProduct />} />
                        <Route
                            path="/addProduit/:productId"
                            element={<AddProduct />}
                        />
                        <Route path="/recherche" element={<SearchBar />} />
                    </Route>
                </Routes>
                <ToastContainer />
            </PanierProvider>
        </BrowserRouter>
    );
}

export default App;
