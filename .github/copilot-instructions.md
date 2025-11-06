## Objetivo

Este arquivo fornece orientações concisas e práticas para agentes de codificação de IA que trabalham no repositório "my-money-family". Concentre-se no layout do repositório, no fluxo de dados (camadas medalhão) e nos fluxos de trabalho do desenvolvedor usados ​​para compilar/implantar código na AWS (ECS, Lambda, S3). Consulte o README do projeto e o arquivo `requirements.txt` para obter detalhes sobre dependências e tempo de execução.

## Arquitetura de alto nível (o que você precisa saber)

- Pipeline estilo Medallion: dados e código são organizados por estágios (raw -> process). Consulte `admin/raw/src` e `admin/process/src`.
- Tempo de execução: o código de processamento é executado dentro de contêineres Docker no AWS ECS. Os contêineres são acionados por uma função Lambda chamada via API Gateway.
- Armazenamento e integrações: o S3 é usado para armazenamento bruto; o Planilhas Google é usado para saída/visualização via `gspread` (consulte `requirements.txt`).

Fluxo simples (do README):
- Cliente -> Gateway de API -> Lambda -> Tarefa ECS -> Contêiner -> S3 / Planilhas Google

## Arquivos e diretórios principais para inspecionar primeiro

- `README.md` — visão geral do projeto e exemplo de endpoint curl (contém o exemplo `curl` e o diagrama da sereia).
- `requirements.txt` — lista completa de dependências do Python (usada para compilações de contêineres e desenvolvimento local).
- `admin/raw/src/` e `admin/process/src/` — código de processamento principal para os estágios raw e process.
- `admin/raw/deploy_docker_ecs.sh` e `admin/process/deploy_docker_ecs.sh` — scripts de implantação usados ​​para compilar/enviar contêineres e registrar tarefas ECS.
- `aws/` — recursos do Terraform: procure em `aws/ecs`, `aws/ec2`, `aws/lambda` e `aws/medalion` para configuração de infraestrutura e convenções de nomenclatura.

## Fluxos de trabalho do desenvolvedor e comandos concretos

- Ambiente local: crie um venv e instale as dependências de `requirements.txt`.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

- Empacotamento do Lambda: o README especifica a criação de um arquivo zip com o conteúdo de `src`. O repositório espera apenas arquivos `.py` no zip do lambda. Exemplo do README:

```bash
cd aws/lambda/src-or-folder
zip -r ../lambda.zip .
zip -g ../lambda.zip lambda_function.py
```

- Implantando contêineres no ECS: inspecione e execute os scripts de implantação dentro de cada diretório de estágio (esses scripts criam a imagem, enviam para um registro e atualizam as definições de tarefas do ECS). Caminhos de exemplo:

```
admin/raw/deploy_docker_ecs.sh
admin/process/deploy_docker_ecs.sh
```

- Acionando uma execução (teste de integração/manual): o README inclui um endpoint funcional do API Gateway e um exemplo de curl. Use-o para confirmar uma execução de ponta a ponta (substitua o ano ou a carga útil conforme necessário):

```bash
curl -X POST "https://0cgzijkxda.execute-api.us-east-1.amazonaws.com/run" \
-H "Content-Type: application/json" \
-d '{"ano": "2025"}'
```

## Convenções e padrões específicos deste repositório

- Layout Medallion/Stage: o código é dividido por stage em `admin/<stage>/src`. Cada stage é empacotado/executado como seu próprio contêiner.
- Infraestrutura como código: os diretórios do Terraform em `aws/` correspondem aos recursos da nuvem; siga as convenções de nomenclatura e caminho ao adicionar a infraestrutura.
- Empacotamento Lambda: o repositório espera um zip mínimo com arquivos .py (o README mostra os comandos zip exatos). Não inclua dependências grandes no zip do lambda — use contêineres ECS para processamento mais pesado.
- Integrações externas: O Planilhas Google é usado para saída via `gspread`/`google-auth` (consulte `requirements.txt`). As credenciais para as APIs do Google são gerenciadas fora do repositório (exceto segredos/variáveis ​​de ambiente configuradas na AWS).

## O que verificar ao fazer edições

- Ao alterar o código de processamento, execute-o localmente com a mesma versão do Python e dependências de `requirements.txt`.
- Se você alterar o comportamento do contêiner, atualize o `deploy_docker_ecs.sh` correspondente e qualquer definição de tarefa do ECS em `aws/ecs`.
- Se estiver adicionando arquivos destinados ao empacotamento do Lambda, siga as instruções zip no README — inclua apenas os arquivos `.py` necessários.

## Dicas rápidas para PRs e depuração

- Inclua um breve resumo de qual estágio (bruto/processo) a alteração afeta e como executar o código localmente ou por meio do endpoint curl fornecido.

- Para alterações de infraestrutura, inclua o caminho do Terraform e descreva quaisquer alterações de estado ou variáveis ​​necessárias.

## Onde não encontramos orientações explícitas

- Não foram encontradas instruções existentes do assistente de IA em nível de repositório. Este arquivo é a orientação canônica de primeira passagem; revise e expanda com segredos, registros e nomenclatura de infraestrutura específicos da equipe, se necessário.

Se algo aqui não estiver claro ou você quiser mais detalhes (por exemplo, etapas de execução/depuração de exemplo para `admin/process/src` ou os argumentos exatos de compilação do Docker usados ​​pelos scripts de implantação), diga-me qual área expandir e eu iterarei.