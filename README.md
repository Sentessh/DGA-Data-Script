<h1>DGA Data Script</h1>

<p>
Este projeto é um pipeline de ETL (Extract, Transform, Load) responsável por coletar dados de partidas de tênis via API,
processá-los e carregá-los em banco de dados MSSQL. O sistema suporta execução incremental diária, controle por data
de referência e integração com dados históricos públicos.
</p>

<hr>

<h2>Principais Funcionalidades</h2>

<ul>
    <li><strong>Execução Incremental:</strong> Processamento baseado em data de referência (executado diariamente).</li>
    <li><strong>Pipeline ETL Completo:</strong> Extração, transformação e carga automatizadas.</li>
    <li><strong>Ordem Controlada de Importação:</strong>
        <ul>
            <li>Players</li>
            <li>Tournaments</li>
            <li>Standings (Rankings)</li>
            <li>Events</li>
            <li>Statistics</li>
        </ul>
    </li>
    <li><strong>Gestão de Rankings:</strong> Inserção automática das siglas ATP/WTA.</li>
    <li><strong>Validação de Torneios:</strong> Criação automática caso não existam.</li>
    <li><strong>Merge Incremental:</strong> Uso de MERGE SQL para evitar duplicidades.</li>
</ul>

<hr>

<h2>Pré-requisitos</h2>

<ul>
    <li>Python 3.8+</li>
    <li>Pip</li>
    <li>Banco MSSQL</li>
</ul>

<hr>

<h2>Instalação</h2>

<h3>1. Clonar repositório</h3>

<pre><code>git clone https://github.com/Sentessh/DGA-Data-Script.git
cd DGA-Data-Script</code></pre>

<h3>2. Instalar dependências</h3>

<pre><code>pip install -r requirements.txt</code></pre>

<h3>3. Configurar variáveis</h3>

<p>Crie um arquivo <strong>.env</strong>:</p>

<pre><code>API_BASE=https://api.api-tennis.com/tennis/
API_KEY=SUA_API_KEY

RAW_DIR=./data/raw
PROCESSED_DIR=./data/processed

DB_DIALECT=mssql+pytds
DB_HOST=HOST
DB_PORT=1433
DB_NAME=DATABASE
DB_USER=USER
DB_PASS=PASSWORD</code></pre>

<hr>

<h2>Estrutura do Projeto</h2>

<pre><code>DGA-Data-Script/
├── src/
│   ├── etl_runner.py
│   ├── fetch_api.py
│   ├── main.py
│   └── sql/
│       ├── merge_players.sql
│       ├── merge_tournaments.sql
│       ├── merge_events.sql
│       ├── merge_rankings.sql
│       └── merge_estatisticas.sql
</code></pre>

<hr>

<h2>Fluxo do ETL</h2>

<h3>1. Extração</h3>
<ul>
    <li>Busca dados da API por data.</li>
</ul>

<h3>2. Transformação</h3>
<ul>
    <li>Normalização de nomes</li>
    <li>Padronização de IDs</li>
    <li>Inclusão de tipo ATP/WTA</li>
</ul>

<h3>3. Carga</h3>
<ul>
    <li>MERGE incremental no banco</li>
    <li>Filtro por <code>data_referencia</code></li>
</ul>

<hr>

<h2>Execução</h2>

<h3>Rodar manualmente</h3>

<pre><code>cd src
python etl_runner.py --date 2025-10-29</code></pre>

<h3>Execução automática (VPS)</h3>

<p>Recomendado agendar via:</p>

<ul>
    <li>Windows Task Scheduler</li>
    <li>CRON (Linux)</li>
</ul>

<hr>

<h2>Execução Incremental</h2>

<p>O pipeline roda diariamente e processa apenas dados da data informada:</p>

<pre><code>WHERE data_referencia = @data_ref</code></pre>

<p>Isso garante:</p>

<ul>
    <li>Alta performance</li>
    <li>Sem duplicidade</li>
    <li>Execução segura diária</li>
</ul>

<hr>

<h2>Integração com Dados Históricos</h2>

<p>O projeto suporta integração com datasets públicos para enriquecimento histórico.</p>

<hr>

<h2>Tecnologias</h2>

<ul>
    <li>Python</li>
    <li>Pandas</li>
    <li>SQLAlchemy</li>
    <li>MSSQL</li>
</ul>

<hr>

<h2>Autor</h2>

<p>Projeto interno DGA — Pipeline de dados esportivos.</p>
