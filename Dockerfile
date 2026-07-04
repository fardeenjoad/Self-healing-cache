FROM node:22-alpine
WORKDIR /app

# Copy dependency manifests first for better layer caching
COPY package.json package-lock.json tsconfig.json ./

# Install all dependencies (devDeps needed for TypeScript build)
RUN npm ci

# Copy source
COPY src/ ./src/

# Compile TypeScript → dist/
RUN npm run build

# Start a cache node — NODE_ID env var selects which node this container is
CMD ["node", "dist/node/CacheNode.js"]
