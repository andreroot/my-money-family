# Imagem base oficial do Python
# FROM python:3.11-slim

# # Evitar buffer no stdout
# ENV PYTHONUNBUFFERED=1

FROM ubuntu:22.04

ENV GOOGLE_APPLICATION_CREDENTIALS="/app/.ssh/my-chave-gcp-devsamelo2.json"
ENV AWS_ACCESS_KEY_ID=
ENV AWS_SECRET_ACCESS_KEY=
ENV AWS_DEFAULT_REGION=us-east-1

# Instalar dependências do sistema (opcional, útil para bash, curl etc.)
RUN apt-get update && apt-get install -y libreoffice
RUN apt-get install -y libreoffice-common libreoffice-calc

# Install dependencies
RUN apt-get install wget -y && \
    apt-get install unzip -y && \
    apt-get install curl -y && \
    apt-get install axel -y && \
    apt-get install python3-venv -y && \
    apt-get install python3-pip -y && \
    pip3 install csvkit

# instalação aws cli
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
    && unzip awscliv2.zip \
    && ./aws/install \
    && rm -rf awscliv2.zip

# # Create a non-root user
# RUN useradd -m -s /bin/bash appuser

# Update package list
RUN apt-get update && apt-get install -y sudo

# Create user with specific UID/GID
RUN groupadd -r appgroup --gid=1001 && \
    useradd -r -g appgroup --uid=1001 --shell=/bin/bash --create-home appuser

# Add user to sudo group (if needed)
RUN usermod -aG sudo appuser

# Set password (for development only)
RUN echo 'appuser:password' | chpasswd

# Set working directory
WORKDIR /app

RUN chown -R appuser:appgroup /app

# Copiar requirements e instalar libs Python
COPY requirements.txt .

# Instalar dependências no sistema (não como user local)
RUN pip install --no-cache-dir -r requirements.txt csvkit==1.0.7 xlrd==1.2.0 openpyxl gspread pandas gspread_dataframe

# Copiar código fonte e scripts
COPY src/ ./src/
# copy shell script
COPY etl_sh/ ./etl_sh/
# Garantir que o entrypoint.sh seja executável
RUN chmod +x ./etl_sh/*.sh

# credencial
RUN mkdir -p .ssh

COPY my-chave-gcp-devsamelo2.json ./.ssh/my-chave-gcp-devsamelo2.json

# # copy input files
# COPY output/ ./output/

RUN mkdir -p /app/output && chmod -R 777 /app/output

# RUN mkdir -p /app/output/original && chown -R appuser:appgroup /app/output/original && chmod -R 777 /app/output/original

# Adicionar ~/.local/bin no PATH
ENV PATH="/home/appuser/.local/bin:${PATH}"

# Switch to the new user
USER appuser

# # Definir entrypoint, recber paratâmetro ano no docker run e passar para o script 
# Exemplo: docker run -it nome_da_imagem 2025
# ENTRYPOINT ["bash", "./etl_sh/etldebs3.sh"]
ENTRYPOINT [""]

# Comando padrão (pode ser sobrescrito no docker run)
CMD [""]