<h1>DGA Data Script</h1>

<p>Este projeto é um pipeline de ETL (Extract, Transform, Load) projetado para extrair dados de partidas de tênis da API Tennis API, processá-los e armazená-los localmente (CSV) ou em banco de dados. O projeto conta também com módulos de integração com dados históricos públicos.</p>

<hr>

<h2>Pré-requisitos</h2>
<ul>
    <li>Python 3.8 ou superior</li>
    <li>Gerenciador de pacotes <code>pip</code></li>
    <li>Sistema Operacional Windows (para execução dos scripts .bat)</li>
</ul>

<hr>

<h2>Instalação e Configuração</h2>

<h3>1. Clonar o repositório</h3>
<pre><code>git clone https://github.com/Sentessh/DGA-Data-Script.git
cd DGA-Data-Script</code></pre>

<h3>2. Instalar dependências</h3>
<p>Recomendamos o uso de um ambiente virtual. Instale as bibliotecas necessárias com:</p>
<pre><code>pip install -r requirements.txt</code></pre>

<h3>3. Configurar Variáveis de Ambiente</h3>
<p>Crie um <strong>arquivo</strong> chamado <code>.env</code> na raiz do projeto. Este arquivo guardará suas chaves e configurações sensíveis.</p>
<p>Copie o conteúdo abaixo e preencha com seus dados:</p>

<pre><code># --- Configurações da API ---
API_BASE=https://api.api-tennis.com/tennis/
API_KEY=SUA_CHAVE_DA_API_AQUI_SEM_ASPAS

# --- Diretórios de Dados ---
RAW_DIR=./data/raw
PROCESSED_DIR=./data/processed

# --- Banco de Dados (MSSQL) ---
DB_DIALECT=mssql+pytds
DB_HOST=seu_host
DB_PORT=1433
DB_NAME=seu_banco
DB_USER=seu_usuario
DB_PASS=sua_senha</code></pre>

<blockquote>
    <strong>Nota:</strong> O arquivo <code>.env</code> não deve ser enviado para o GitHub (certifique-se de que ele esteja listado no seu <code>.gitignore</code>).
</blockquote>

<h3>4. Estrutura de Pastas</h3>
<p>Para garantir que o script funcione corretamente, a estrutura deve ser:</p>

<pre><code>DGA-Data-Script/
├── src/
│   └── data/
│       ├── raw/         # Onde os dados brutos (JSON/CSV) serão salvos
│       └── processed/   # Onde os dados tratados serão salvos</code></pre>

<hr>

<h2>Scripts e Funcionalidades Adicionais</h2>

<h3>Arquivo .bat (Backfill de Histórico)</h3>
<p>O arquivo <code>backfill_historico.bat</code> é um script em lote do Windows destinado à automação de cargas históricas.</p>
<ul>
    <li><strong>Função:</strong> Executa o <code>etl_runner.py</code> repetidamente para um intervalo de datas definido.</li>
    <li><strong>Uso:</strong> Ideal para preencher o banco de dados com partidas passadas sem a necessidade de executar o comando Python manualmente para cada dia.</li>
    <li><strong>Como editar:</strong> Abra o arquivo em um editor de texto e ajuste o loop <code>FOR</code> com as datas de início e fim desejadas.</li>
</ul>

<h3>Integração "Sync Sackman"</h3>
<p>Este módulo realiza a sincronização com os dados públicos de Jeff Sackmann (mantenedor dos repositórios <code>tennis_atp</code> e <code>tennis_wta</code>).</p>
<ul>
    <li><strong>Objetivo:</strong> Enriquecer a base de dados proprietária cruzando informações com o vasto histórico gratuito mantido pela comunidade do Sackmann.</li>
    <li><strong>Funcionamento:</strong> O script baixa ou atualiza os arquivos CSV do repositório público e padroniza os nomes dos jogadores e torneios para permitir a unificação com os dados da API paga.</li>
</ul>

<hr>

<h2>Como Usar</h2>

<p>O script principal é o <code>etl_runner.py</code>, localizado na pasta <code>src</code>.</p>

<h3>Execução via Linha de Comando</h3>
<p>Navegue até a pasta <code>src</code> e execute o comando especificando a data:</p>

<pre><code>cd src
python etl_runner.py --date 2025-10-29</code></pre>

<h3>Argumentos</h3>
<ul>
    <li><strong>--date</strong>: (Obrigatório) Data de referência (YYYY-MM-DD).</li>
    <li><strong>--load-mode</strong>: (Opcional) Define o modo de escrita no banco (<code>replace</code>, <code>append</code>).</li>
</ul>

<hr>

<h2>Tecnologias</h2>
<ul>
    <li><strong>Linguagem:</strong> Python</li>
    <li><strong>Manipulação de Dados:</strong> Pandas</li>
    <li><strong>Banco de Dados:</strong> SQLAlchemy (MSSQL)</li>
    <li><strong>Automação:</strong> Batch Scripting (.bat)</li>
</ul>
