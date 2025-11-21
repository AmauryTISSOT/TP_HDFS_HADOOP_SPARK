import express from "express";
import dotenv from "dotenv";
import cors from "cors";
import connectDB from "./db/DbConnect.js";
import routes from "./routing/ProduitRoute.js";
import { connectProducer } from "./kafka/producer.js";

dotenv.config();
connectDB();

const app = express();

const port = process.env.PORT;
app.use(express.json());
app.use(cors());

app.use(routes);

const startServer = async () => {
    try {
        await connectProducer();
        app.use(routes);
        app.listen(port, () =>
            console.log(`Le serveur est à l'écoute sur le port ${port}`)
        );
    } catch (err) {
        console.error("Erreur lors du démarrage :", err);
    }
};

startServer();
