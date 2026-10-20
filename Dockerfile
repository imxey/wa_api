FROM node:25
WORKDIR /app
COPY . .
RUN npm install
CMD ["node", "index.js"]
