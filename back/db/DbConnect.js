import mongoose from "mongoose";
import dotenv from "dotenv";


dotenv.config();
/**
 * Fonction permettant la connection à notre base de donnée
 */
const connectDB = async () => {
    try {
        console.log(process.env.DB);
        await mongoose.connect(process.env.DB);
        console.log('MongoDB connecté');
    } catch (error) {
        console.error('Problème lors de la connection ', error);
        process.exit(1);
    }
}

export default connectDB