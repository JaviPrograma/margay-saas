@echo off
setlocal

REM === CONFIGURAR ESTOS DOS SI HACE FALTA ===
set PORT=9222
set PROFILE=Default

REM Chrome.exe (64-bit por defecto). Cambiá si tenés otra ruta.
set CHROME="C:\Program Files\Google\Chrome\Application\chrome.exe"

REM Tu carpeta del proyecto (¡con comillas por los espacios!)
set PROYECTO="C:\Users\Javi 0o\Desktop\Margay"

echo Cerrando Chrome (si estuviera abierto)...
taskkill /F /IM chrome.exe >NUL 2>&1

echo Abriendo Chrome con depuracion en puerto %PORT% y perfil %PROFILE%...
start "" %CHROME% --remote-debugging-port=%PORT% --profile-directory="%PROFILE%" --new-window https://web.whatsapp.com

REM Espera corta para que levante el puerto
timeout /t 4 /nobreak >NUL

REM Variable que usa recordatorios.py (ya viene con default 127.0.0.1:9222)
set WA_DEBUG_ADDR=127.0.0.1:%PORT%

echo Iniciando Margay...
cd /d %PROYECTO%

REM (Opcional) activar venv si existe
if exist venv\Scripts\activate.bat call venv\Scripts\activate.bat

REM Asegurar dependencias basicas (rapido y silencioso)
python -m pip install --disable-pip-version-check -q selenium webdriver-manager

python app.py

endlocal
