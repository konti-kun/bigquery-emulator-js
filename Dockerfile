FROM node:22-alpine AS base

RUN apk update && \
    apk add --no-cache --repository http://dl-cdn.alpinelinux.org/alpine/v3.18/main make gcc g++ python3~=3.11.12

COPY . /app
WORKDIR /app

FROM base AS prod-deps
RUN npm install

FROM base AS build
RUN npm install
RUN npm run build

FROM base
COPY --from=prod-deps /app/node_modules /app/node_modules
COPY --from=build /app/build /app/build

EXPOSE 9050
CMD [ "npm", "run", "start"]
