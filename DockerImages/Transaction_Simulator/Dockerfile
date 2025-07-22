# Use Node.js base image
FROM node:18-alpine

# Set working directory inside the container
WORKDIR /app

# Copy package.json and install dependencies
COPY package*.json ./
RUN npm install

# Copy the rest of the app (including index.js and data.csv)
COPY producer.js .
COPY data.csv .

# Default command
CMD ["node", "producer.js"]
