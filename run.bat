set input_file=input.gsheet
set output_file=output.xlsx
call python gsheet_to_xlsx.py %input_file%  %output_file% > log.txt
