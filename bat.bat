@echo off
set CHROME="%ProgramFiles%\Google\Chrome\Application\chrome.exe"
if not exist %CHROME% set CHROME="%ProgramFiles(x86)%\Google\Chrome\Application\chrome.exe"
if not exist %CHROME% set CHROME=chrome
start "" %CHROME% --remote-debugging-port=9222 --user-data-dir="%LOCALAPPDATA%\WhatsAppSession" --profile-directory="Default" https://web.whatsapp.com
