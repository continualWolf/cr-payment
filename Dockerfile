ARG NODE_VERSION=22.20.0

FROM node:${NODE_VERSION} AS builder

WORKDIR /app

COPY package*.json ./
RUN npm install --production

COPY . .

CMD ["node", "server.js"]