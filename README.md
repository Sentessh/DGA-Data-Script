<div>

  <h1>Configuração do Projeto</h1>

  <h2>Criação do arquivo <code>.env</code></h2>

  <p>Crie uma pasta chamada <code>.env</code> e coloque dentro dela o seguinte conteúdo:</p>

  <pre><code>===============================================
API_BASE=https://api.api-tennis.com/tennis/
API_KEY="BOTA A CHAVE DA API AQ, SEM AS ASPAS"

RAW_DIR=./data/raw
PROCESSED_DIR=./data/processed

DB_DIALECT=mssql+pytds
DB_HOST=
DB_PORT=
DB_NAME=
DB_USER=
DB_PASS=
===============================================</code></pre>

  <hr />

  <h2>Execução do Script</h2>

  <p>Pra rodar é aquele esquema: modifica o comando abaixo pra data que você quiser.</p>

  <p>Esse aqui funciona quando você está <strong>dentro da pasta <code>src</code></strong>, mas se não estiver, é só ajustar o caminho até o arquivo <code>etl_runner.py</code>, tlgd?</p>

  <pre><code>python etl_runner.py --date 2025-10-29 --load-mode replace</code></pre>

  <p><strong>Atenção:</strong> O <code>--load-mode</code> não tá funcionando, não sei por quê, acho que configurei errado ou perdemos acesso ao banco de dados kk</p>

  <hr />

  <h2>Estrutura de Diretórios</h2>

  <p>Caso o código reclame que não tem onde salvar os CSVs, é só criar a pasta <code>data</code> dentro de <code>src</code>, e dentro dela criar mais duas pastas:</p>

  <ul>
    <li><code>raw</code></li>
    <li><code>processed</code></li>
  </ul>

  <p>Assim deve funcionar de boa :)</p>

</div>
