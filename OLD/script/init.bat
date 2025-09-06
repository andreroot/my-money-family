echo off
cls
REM execução do arquivo na pasta raiz .\admin\src\script
REM cd C:\\Users\andre\Documents\github\my-money-family
REM PATH C:\Users\andre\Documents\github\my-money-family\admin\src\script\init.bat
REM EXEC .\admin\src\script\init.bat > .\admin\src\script\log\log.txt
echo "EXECUTAR PROCESSO: "%cd%
echo "INICIAR PROCESSO..."
REM execução de comandos no powershell
powershell Set-ExecutionPolicy RemoteSigned
powershell.exe Get-Date
powershell.exe Write-Output "EXECUÇÃO DEBITO" 
powershell.exe .\admin\src\script\exec_main_debito.ps1 > .\admin\src\script\log\log_deb.txt
@REM powershell.exe .\admin\src\script\exec_main_credito.ps1 > .\admin\src\script\log\log_cred.txt
