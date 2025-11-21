FROM node:20-slim

WORKDIR /app

COPY package*.json ./

RUN npm install

COPY . .

EXPOSE 5043

CMD ["bash", "-c", "npm run seed && npm start"]
