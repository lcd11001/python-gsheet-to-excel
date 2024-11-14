import json
import os
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import pandas as pd

def gsheet_to_xlsx(gsheet_path, output_path):
    """
    Convert a .gsheet file to .xlsx format, handling column mismatches
    
    Parameters:
    gsheet_path (str): Path to the .gsheet file
    output_path (str): Path where the .xlsx file should be saved
    """
    # Read the .gsheet file
    with open(gsheet_path, 'r') as f:
        gsheet_data = json.load(f)
    
    print(f"* gsheet_data {gsheet_data}")
    
    # Extract the spreadsheet ID
    spreadsheet_id = gsheet_data.get('doc_id')
    print(f"* spreadsheet_id {spreadsheet_id}")
    
    if not spreadsheet_id:
        raise ValueError("Could not find spreadsheet ID in .gsheet file")
    
    # OAuth 2.0 credentials
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
    
    flow = InstalledAppFlow.from_client_secrets_file(
        'credentials.json',
        SCOPES
    )
    creds = flow.run_local_server(port=0)
    
    try:
        # Build the Sheets API service
        service = build('sheets', 'v4', credentials=creds)
        
        # Get all sheets in the spreadsheet
        spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheets = spreadsheet.get('sheets', [])
        
        if not sheets:
            raise ValueError("No sheets found in the spreadsheet")
            
        # Filter for visible sheets
        visible_sheets = [
            sheet for sheet in sheets 
            if not sheet.get('properties', {}).get('hidden', False)
        ]
        
        if not visible_sheets:
            print("* Warning: All sheets are hidden. Attempting to use all sheets instead.")
            visible_sheets = sheets
        
        print(f"* Found {len(visible_sheets)} sheet(s)")
        
        # Create Excel writer
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            sheets_processed = 0
            
            for sheet in visible_sheets:
                properties = sheet['properties']
                sheet_name = properties['title']
                print(f"* Processing sheet: {sheet_name}")
                
                try:
                    sheet_range = f'{sheet_name}!A1:ZZ'
                    
                    # Get the sheet data
                    result = service.spreadsheets().values().get(
                        spreadsheetId=spreadsheet_id,
                        range=sheet_range
                    ).execute()
                    
                    values = result.get('values', [])
                    
                    if not values:
                        print(f"* Sheet '{sheet_name}' is empty, skipping")
                        continue

                    # Get the maximum number of columns in any row
                    max_cols = max(len(row) for row in values)
                    
                    # Get headers from first row, pad if necessary
                    headers = values[0] if values else []
                    if len(headers) < max_cols:
                        # Pad headers with unnamed columns
                        headers.extend([f'Unnamed_{i+1}' for i in range(len(headers), max_cols)])
                    
                    # Pad all rows to have the same number of columns
                    padded_values = []
                    for row in values[1:]:
                        if len(row) < max_cols:
                            padded_row = row + [''] * (max_cols - len(row))
                        else:
                            padded_row = row
                        padded_values.append(padded_row)
                    
                    print(f"* Number of columns: {max_cols}")
                    print(f"* Number of rows: {len(padded_values)}")
                    
                    # Convert to DataFrame with padded values
                    df = pd.DataFrame(padded_values, columns=headers)
                    
                    # Write to Excel
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                    sheets_processed += 1
                    print(f"* Successfully processed sheet: {sheet_name}")
                    
                except Exception as e:
                    print(f"* Error processing sheet '{sheet_name}': {str(e)}")
                    continue
            
            if sheets_processed == 0:
                raise ValueError("No sheets could be processed successfully")
        
        print(f"* Successfully converted {gsheet_path} to {output_path}")
        print(f"* Total sheets processed: {sheets_processed}")
        
    except Exception as e:
        print(f"* An error occurred: {str(e)}")
        raise

# Example usage
if __name__ == "__main__":
    gsheet_to_xlsx("input.gsheet", "output.xlsx")