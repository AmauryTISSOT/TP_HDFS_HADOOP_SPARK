import express from "express";
import dotenv from "dotenv";
import cors from "cors";
import connectDB from "./db/DbConnect.js";
import routes from "./routing/ProduitRoute.js";

dotenv.config();
connectDB();

const app = express();

const port = process.env.PORT;
app.use(express.json());
app.use(cors());

app.use(routes);

app.listen(port, () =>
    console.log(`Le serveur est a l'écoute sur le port ${port}`)
);
