import json
import os
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
import pandas as pd
import pickle

#define a constant string
YES = 'Có'
NO = 'Không'

def excel_col_to_index(col_str):
    """
    Convert Excel column letters to 0-based column index.
    For example: A -> 0, B -> 1, Z -> 25, AA -> 26, AB -> 27, etc.
    """
    result = 0
    for char in col_str:
        result = result * 26 + (ord(char.upper()) - ord('A') + 1)
    return result - 1

def process_sheet_data(values, column_mapping):
    """
    Process sheet data into a pandas DataFrame with proper column mapping
    """
    if not values:
        return None

    # Get the maximum number of columns in any row
    max_cols = max(len(row) for row in values)
    
    # Get headers from first row, pad if necessary
    headers = values[0] if values else []
    if len(headers) < max_cols:
        headers.extend([f'Unnamed_{i+1}' for i in range(len(headers), max_cols)])
    
    # Pad all rows to have the same number of columns
    padded_values = []
    for row in values[1:]:
        padded_row = row + [''] * (max_cols - len(row)) if len(row) < max_cols else row
        padded_values.append(padded_row)

    split_dfs = []
    for idx, row in enumerate(padded_values, start=1):
        try:
            # Extract common info using the correct column indices
            common_info = []
            for col in column_mapping['src_common_info_id']:
                col_idx = excel_col_to_index(col)
                common_info.append(row[col_idx] if col_idx < len(row) else '')
            
            # Replace the first element with the row index
            common_info[0] = idx
            
            # Extract owner info using the correct column indices
            owner_info = []
            for col in column_mapping['src_owner_info_id']:
                col_idx = excel_col_to_index(col)
                owner_info.append(row[col_idx] if col_idx < len(row) else '')
            # Append the last element with 'Owner'
            owner_info.append('Owner')

            # Create DataFrame for the row
            row_data = [common_info + owner_info]
            row_columns = column_mapping['dest_common_info_names'] + column_mapping['dest_member_info_names']
            row_df = pd.DataFrame(row_data, columns=row_columns)
            split_dfs.append(row_df)

            # Extract member info using the correct column indices
            hasMember = row[excel_col_to_index(column_mapping['src_owner_info_next_id'])] == YES
            if hasMember:
                member_mapping = {
                    'src_member_info_next_id': column_mapping['src_member_info_next_id'],
                    'src_member_info_names': column_mapping['src_member_info_names'],
                    'src_member_info_id': column_mapping['src_member_info_id'],
                    'dest_member_info_ids': column_mapping['dest_member_info_ids'],
                    'dest_member_info_names': column_mapping['dest_member_info_names']
                }
                members_info = process_member_data(row, member_mapping)

                for member_info in members_info:
                    row_member_data = [common_info + member_info]
                    row_member_columns = column_mapping['dest_common_info_names'] + column_mapping['dest_member_info_names']
                    row_member_df = pd.DataFrame(row_member_data, columns=row_member_columns)
                    split_dfs.append(row_member_df)
            
        except Exception as e:
            print(f"* Error processing row {idx}: {str(e)}")
            continue

    if not split_dfs:
        return None

    return pd.concat(split_dfs, ignore_index=True)

def process_member_data(row, member_mapping):
    """
    Process member data into a list of rows for the member DataFrame
    """
    member_info = []
    member_info_next_id = member_mapping['src_member_info_next_id']
    member_info_names = member_mapping['src_member_info_names']
    member_info_ids = member_mapping['src_member_info_id']
    member_info_next_col = excel_col_to_index(member_info_next_id)
    member_info_columns = member_mapping['dest_member_info_names']
    
    col_offset = 0

    while True:
        member_data = []
        
        # Extract member info using the correct column indices
        for col in member_info_ids:
            col_idx = excel_col_to_index(col) + col_offset
            member_data.append(row[col_idx] if col_idx < len(row) else '')
        # swap the last two elements
        # because the last element is the phone number
        member_data[-1], member_data[-2] = member_data[-2], member_data[-1]
        
        # Append the member info to the list
        member_info.append(member_data)
        
        # Check if there are more members
        if member_info_next_col >= len(row) or row[member_info_next_col] == NO:
            break

        # Move to the next member
        col_offset  += len(member_info_ids) + 1
        member_info_next_col += col_offset

    return member_info


def get_credentials():
    """
    Get cached credentials or create new ones if needed.
    """
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
    creds = None
    token_path = 'token.pickle'
    
    if os.path.exists(token_path):
        with open(token_path, 'rb') as token:
            creds = pickle.load(token)
    
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        
        with open(token_path, 'wb') as token:
            pickle.dump(creds, token)
    
    return creds

def gsheet_to_xlsx(gsheet_path, output_path, column_mapping):
    """
    Convert a .gsheet file to .xlsx format, handling column mismatches
    """
    try:
        # Read the .gsheet file
        with open(gsheet_path, 'r') as f:
            gsheet_data = json.load(f)
        
        spreadsheet_id = gsheet_data.get('doc_id')
        if not spreadsheet_id:
            raise ValueError("Could not find spreadsheet ID in .gsheet file")
        
        # Get cached credentials
        creds = get_credentials()
        
        # Build the Sheets API service
        service = build('sheets', 'v4', credentials=creds)
        
        # Get all sheets in the spreadsheet
        spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheets = spreadsheet.get('sheets', [])
        
        if not sheets:
            raise ValueError("No sheets found in the spreadsheet")
        
        # Filter for visible sheets
        visible_sheets = [sheet for sheet in sheets 
                        if not sheet.get('properties', {}).get('hidden', False)]
        
        if not visible_sheets:
            print("* Warning: All sheets are hidden. Attempting to use all sheets instead.")
            visible_sheets = sheets
        
        print(f"* Found {len(visible_sheets)} sheet(s)")
        
        # Create Excel writer
        writer = pd.ExcelWriter(output_path, engine='openpyxl')
        sheets_processed = 0
        
        try:
            for sheet in visible_sheets:
                properties = sheet['properties']
                sheet_name = properties['title']
                print(f"* Processing sheet: {sheet_name}")
                
                try:
                    # Get the sheet data
                    result = service.spreadsheets().values().get(
                        spreadsheetId=spreadsheet_id,
                        range=f'{sheet_name}!A1:ZZ'
                    ).execute()
                    
                    values = result.get('values', [])
                    if not values:
                        print(f"* Sheet '{sheet_name}' is empty, skipping")
                        continue
                    
                    # Process the sheet data
                    final_df = process_sheet_data(values, column_mapping)
                    if final_df is None:
                        print(f"* No valid data in sheet '{sheet_name}', skipping")
                        continue
                    
                    # Write to Excel
                    final_df.to_excel(writer, sheet_name=sheet_name, index=False)
                    sheets_processed += 1
                    print(f"* Successfully processed sheet: {sheet_name}")
                    
                except Exception as e:
                    print(f"* Error processing sheet '{sheet_name}': {str(e)}")
                    continue
            
            if sheets_processed == 0:
                raise ValueError("No sheets could be processed successfully")
                       
            print(f"* Successfully converted {gsheet_path} to {output_path}")
            print(f"* Total sheets processed: {sheets_processed}")
            
        finally:
            # Make sure we close the writer even if an error occurs
            if writer:
                writer.close()
                
    except Exception as e:
        print(f"* An error occurred: {str(e)}")
        raise

# Example usage showing how to handle columns beyond Z
if __name__ == "__main__":
    column_mapping = {
        'src_common_info_id': ['A', 'B', 'C', 'D', 'E'],
        'src_common_info_names': ['Timestamp', 'Block', 'Floor', 'HomeID', 'Owner'],
        
        'src_owner_info_id': ['F', 'G', 'H', 'I'],
        'src_owner_info_names': ['Full Name', 'Sex', 'Birthday', 'Phone'],
        
        'src_owner_info_next_id': 'J',

        'src_member_info_id': ['K', 'L', 'M', 'N', 'O'],
        'src_member_info_names': ['Full Name', 'Sex', 'Birthday', 'Relationship', 'Phone'],
        'src_member_info_next_id': 'P',
        
        'dest_common_info_ids': ['A', 'B', 'C', 'D', 'E'],
        'dest_common_info_names': ['Counting', 'Block', 'Floor', 'HomeID', 'Owner'],
        
        'dest_member_info_ids': ['F', 'G', 'H', 'I', 'K'],
        'dest_member_info_names': ['Full Name', 'Sex', 'Birthday', 'Phone', 'Relationship'],
    }

    gsheet_to_xlsx("input.gsheet", "output.xlsx", column_mapping)

