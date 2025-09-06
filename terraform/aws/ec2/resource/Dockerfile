FROM ubuntu:22.04

USER root

# ARG GITHUB_TOKEN

# Instala dependências básicas
RUN apt-get update && \
    apt-get install -y curl git sshpass openssh-client
# apt-get install -y curl python3 python3-pip python3-venv git sshpass openssh-client


# Instala Node.js (LTS) e npm
RUN curl -fsSL https://deb.nodesource.com/setup_lts.x | bash - && \
    apt-get install -y nodejs

# Instala n8n globalmente
RUN npm install -g npm@latest && \
    npm cache clean --force && \
    npm install -g n8n --unsafe-perm \
    npm install -g bcryptjs

# Adiciona virtualenv ao PATH
ENV PATH="/opt/venv/bin:$PATH"

RUN useradd --create-home --shell /bin/bash node

# Instala o node WAHA dentro do diretório custom
WORKDIR /home/node/.n8n/custom

# install @devlikeapro/n8n-nodes-waha

RUN npm init -y && \
    npm install https://github.com/devlikeapro/n8n-nodes-waha.git

# Define a variável de ambiente pra N8N carregar extensões
ENV N8N_CUSTOM_EXTENSIONS=/home/node/.n8n/custom

# Instale tini (init system para containers)
RUN apt-get update && apt-get install -y tini

# USER node

RUN chown -R node:node /home/node/.n8n

ENTRYPOINT ["tini", "--"]
CMD ["n8n"]
