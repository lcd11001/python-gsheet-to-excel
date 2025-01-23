@echo off
rem Set the code page to UTF-8
chcp 65001 > nul
echo Running the script...
call python gsheet_to_xlsx.py "Khảo sát nhân khẩu KP 23 CCCD 2025.gsheet" "output.xlsx" > log.txt
pause