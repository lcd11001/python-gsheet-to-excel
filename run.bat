@echo off
rem Set the code page to UTF-8
chcp 65001 > nul
call python gsheet_to_xlsx.py "Khảo sát nhân khẩu KP 23 CCCD.gsheet" "output.xlsx" > log.txt
pause