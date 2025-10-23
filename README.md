<h2>Visão Geral</h2>
<p>
Script único (<code>fetch_api.py</code>) que faz chamadas diretas a poucos endpoints da API 
(<em>players</em>, <em>fixtures</em>, <em>odds</em>), valida a resposta e salva CSVs com colunas estáveis.
Sem concorrência, sem ETL complexo. Pensado para rodar todo dia e, depois, plugar em Airbyte ou banco.
</p>

<h2>Estrutura</h2>
<pre>
api-minimal/
├─ fetch_api.py
├─ .env.example
└─ data/              # saídas .csv com timestamp UTC no nome
</pre>

<h2>Requisitos</h2>
<ul>
  <li>Python 3.12+ (funciona em 3.13/3.14)</li>
  <li>Pacotes: <code>requests</code>, <code>pandas</code>, <code>python-dotenv</code></li>
</ul>

<h3>Instalação (Windows PowerShell)</h3>
<pre><code>py -m venv .venv
.\.venv\Scripts\activate
py -m pip install -U pip
pip install requests pandas python-dotenv
</code></pre>

<h2>Configuração (.env)</h2>
<p>Copie <code>.env.example</code> para <code>.env</code> e ajuste:</p>
<pre><code>API_BASE=https://api.api-tennis.com/tennis/
API_KEY=COLOQUE_SUA_CHAVE_AQUI
OUTPUT_DIR=./data
REQUEST_TIMEOUT=20
REQUEST_SLEEP_SECONDS=0.6
</code></pre>

<h2>Como Rodar</h2>

<h3>Coletar tudo de um dia (fixtures + odds)</h3>
<pre><code>python fetch_api.py --date 2025-10-22 --sort-cols
</code></pre>

<h3>Fixtures em intervalo (inclusivo)</h3>
<pre><code>python fetch_api.py --fixtures --date-start 2025-10-20 --date-stop 2025-10-22 --sort-cols
</code></pre>

<h3>Odds por torneio em um dia</h3>
<pre><code>python fetch_api.py --odds --date 2025-10-22 --tournament_key 1234 --sort-cols
</code></pre>

<h3>Player específico (se a API exigir <code>player_key</code>)</h3>
<pre><code>python fetch_api.py --players --player_key 1905 --sort-cols
</code></pre>

<h2>Saída</h2>
<ul>
  <li>Arquivos CSV com timestamp UTC no nome (ex.: <code>fixtures_2025-10-22T215606Z.csv</code>).</li>
  <li>Cada execução gera <strong>novo arquivo</strong> (não sobrescreve).</li>
  <li>Ordem de colunas estável: chaves comuns primeiro, demais em ordem alfabética.</li>
</ul>

<h2>Flags e Comportamento</h2>
<ul>
  <li><code>--date</code> → usa a mesma data para <code>date_start</code>/<code>date_stop</code>.</li>
  <li><code>--date-start</code> / <code>--date-stop</code> → define intervalo explícito.</li>
  <li><code>--sort-cols</code> → ordena colunas alfabeticamente (além da ordem estável padrão).</li>
  <li>Sem parâmetros obrigatórios → o endpoint é <em>pulado</em> (execução não quebra).</li>
  <li>Respostas com <code>success != 1</code> → erro claro no console com parte do corpo para diagnóstico.</li>
</ul>

<h2>Exemplos de Logs</h2>
<pre><code>[REQ] method=get_fixtures params={'method': 'get_fixtures', 'APIkey': '***', 'date_start': '2025-10-22', 'date_stop': '2025-10-22'}
[OK] fixtures   | registros: 523    | arquivo: fixtures_2025-10-22T215606Z.csv
[OK] odds       | registros: 1      | arquivo: odds_2025-10-22T215610Z.csv
[FIM] Execução concluída com sucesso.
</code></pre>

<h2>Troubleshooting Rápido</h2>
<ul>
  <li><strong>Erro players:</strong> <code>Required parameter missing: player_key</code> → rode com <code>--player_key</code> ou deixe sem <code>--players</code>.</li>
  <li><strong>Erro fixtures/odds:</strong> pede <code>date_start</code>/<code>date_stop</code> → use <code>--date</code> ou passe ambas.</li>
  <li><strong>Timeouts:</strong> aumente <code>REQUEST_TIMEOUT</code> no <code>.env</code> e/ou aumente <code>REQUEST_SLEEP_SECONDS</code>.</li>
  <li><strong>CSV vazio:</strong> API pode não ter dados para o filtro; verifique parâmetros e datas.</li>
</ul>

<h2>Agendamento (Windows)</h2>
<p>Use o Agendador de Tarefas para rodar diariamente (exemplo):</p>
<pre><code>Program/script:   C:\caminho\para\python.exe
Add arguments:    C:\Users\Bernardo\Desktop\DGA\fetch_api.py --date 2025-10-22 --sort-cols
Start in:         C:\Users\Bernardo\Desktop\DGA
</code></pre>

<h2>Dicas de Qualidade</h2>
<ul>
  <li>Mantenha os filtros o mais simples possível (API mínima = menos chance de travar).</li>
  <li>Faça commit dos scripts, mas <strong>não</strong> commite o <code>.env</code>.</li>
  <li>Prefira datas exatas (ex.: <code>YYYY-MM-DD</code>) para reprodutibilidade.</li>
</ul>