@echo off
:: Garante que o script rode a partir da pasta onde o arquivo esta salvo
cd /d "%~dp0"

echo ========================================================
echo INICIANDO BACKFILL DE DADOS
echo ========================================================
echo.

:: O comando abaixo usa PowerShell para:
:: 1. Fazer um loop de 0 a 59 (60 dias).
:: 2. Calcular a data (Hoje - X dias).
:: 3. Chamar o seu etl_runner.py para aquela data.
:: 4. Esperar 2 segundos entre cada dia para nao bloquear sua API Key.

powershell -Command "0..99 | ForEach-Object { $dt = (Get-Date).AddDays(-$_).ToString('yyyy-MM-dd'); Write-Host '>>> PROCESSANDO DATA:' $dt -ForegroundColor Cyan; python src/etl_runner.py --date $dt --db; Write-Host '...Aguardando cooldown da API...' -ForegroundColor DarkGray; Start-Sleep -Seconds 2 }"

echo.
echo ========================================================
echo FIM DO PROCESSO. VERIFIQUE O BANCO DE DADOS.
echo ========================================================
pause