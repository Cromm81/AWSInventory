import boto3
import pandas as pd
import os
import shutil
import time
from datetime import datetime
from botocore.exceptions import ClientError

# === SETUP ===
base_path = r'C:\temp\matrix'
archive_path = os.path.join(base_path, 'archive')
log_file = os.path.join(base_path, 'inventory_log.txt')
os.makedirs(archive_path, exist_ok=True)

rsa_file = os.path.join(base_path, 'rsadump.csv')
qualys_file = os.path.join(base_path, 'qualysdump.csv')
ws_file = os.path.join(base_path, 'workspaces_dump.csv')
output_excel = os.path.join(base_path, 'workspace_inventory_master.xlsx')


# === LOGGING ===
# FIX: encoding='utf-8' prevents UnicodeEncodeError on emoji/special characters
def log(msg):
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(f"{datetime.now()} - {msg}\n")
    print(msg)


# === PRE-FLIGHT CHECKS ===
# IMPROVEMENT: Validate all input files exist before doing any work
def preflight_checks():
    missing = []
    for label, path in [('RSA dump', rsa_file), ('Qualys dump', qualys_file)]:
        if not os.path.exists(path):
            missing.append(f"  - {label}: {path}")
    if missing:
        log("[FATAL] Missing input files. Aborting.")
        for m in missing:
            log(m)
        raise FileNotFoundError("One or more required input files are missing.")
    log("[OK] Pre-flight checks passed. All input files found.")


# === FLATTEN UTILITY ===
def flatten_dict(d, parent_key='', sep='.'):
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key.lower(), v))
    return dict(items)


# === SAFE TAG FETCH ===
# IMPROVEMENT: Log the workspace ID that exhausted all retries for traceability
def safe_get_tags(client, ws_id, retries=3):
    for attempt in range(retries):
        try:
            return client.describe_tags(ResourceId=ws_id).get('TagList', [])
        except ClientError as e:
            log(f"[!] Retrying tag fetch for {ws_id} (attempt {attempt + 1}): {e}")
            time.sleep(1)
        except Exception as e:
            log(f"[!] Unexpected error fetching tags for {ws_id}: {e}")
            break
    log(f"[!] Tag fetch failed permanently for {ws_id} after {retries} attempts.")
    return []


# === WORKSPACES API ===
def pull_workspaces():
    log("Pulling WorkSpaces data from AWS...")
    client = boto3.client('workspaces', region_name='ca-central-1')
    workspaces = []
    next_token = None

    while True:
        response = (
            client.describe_workspaces(NextToken=next_token)
            if next_token
            else client.describe_workspaces()
        )
        workspaces.extend(response['Workspaces'])
        next_token = response.get('NextToken')
        if not next_token:
            break

    log(f"[OK] Retrieved {len(workspaces)} workspaces from AWS.")

    flattened_rows = []
    all_columns = set()

    for ws in workspaces:
        flat = flatten_dict(ws)
        ws_id = ws.get('WorkspaceId')
        tags = safe_get_tags(client, ws_id)
        for tag in tags:
            flat[f'tags.{tag["Key"].lower()}'] = tag['Value']
        flattened_rows.append(flat)
        all_columns.update(flat.keys())

    normalized_rows = [
        {col: row.get(col, None) for col in all_columns}
        for row in flattened_rows
    ]
    ws_df = pd.DataFrame(normalized_rows)

    # Filter only EC2AMAZ / WSAMZN hostnames
    if 'computername' in ws_df.columns:
        ws_df = ws_df[
            ws_df['computername'].astype(str).str.startswith(('EC2AMAZ', 'WSAMZN'), na=False)
        ]

    ws_df.columns = [col.strip().lower() + '_vdi' for col in ws_df.columns]
    ws_df.to_csv(ws_file, index=False)
    log(f"[OK] WorkSpaces data saved: {len(ws_df)} rows.")
    return ws_df


# === LOAD OTHER SOURCES ===
def load_sources():
    log("Loading RSA and Qualys files...")
    rsa_df = pd.read_csv(rsa_file, skiprows=3)
    qualys_df = pd.read_csv(qualys_file, skiprows=3)

    rsa_df.columns = [col.strip().lower() for col in rsa_df.columns]
    qualys_df.columns = [col.strip().lower() for col in qualys_df.columns]

    rsa_df.rename(columns={'user id': 'username'}, inplace=True)

    # FIX: was str[e] (NameError) — must be str[0]
    qualys_df.rename(
        columns={qualys_df.columns[2]: 'agent host', qualys_df.columns[3]: 'ipv4'},
        inplace=True
    )
    qualys_df['short_host'] = qualys_df['agent host'].astype(str).str.split('.').str[0]

    # IMPROVEMENT: Drop Qualys duplicates before merging to prevent inflated row counts
    before = len(qualys_df)
    qualys_df = qualys_df.drop_duplicates(subset=['short_host'])
    dropped = before - len(qualys_df)
    if dropped > 0:
        log(f"[WARN] Dropped {dropped} duplicate Qualys entries by short_host.")

    qualys_df = qualys_df[
        qualys_df['short_host'].str.startswith(('EC2AMAZ', 'WSAMZN'), na=False)
    ]

    qualys_df.columns = [col + '_qualys' for col in qualys_df.columns]
    rsa_df.columns = [col + '_rsa' for col in rsa_df.columns]

    log(f"[OK] RSA rows: {len(rsa_df)} | Qualys rows: {len(qualys_df)}")
    return rsa_df, qualys_df


# === MERGE PHASE ===
def merge_datasets(ws_df, rsa_df, qualys_df):
    log("Merging datasets...")

    # FIX: Both merge calls were missing closing parentheses in original
    merged = pd.merge(
        ws_df,
        qualys_df,
        left_on=['computername_vdi', 'ipaddress_vdi'],
        right_on=['short_host_qualys', 'ipv4_qualys'],
        how='outer'
    )

    merged = pd.merge(
        merged,
        rsa_df,
        left_on='username_vdi',
        right_on='username_rsa',
        how='left'
    )

    merged.reset_index(drop=True, inplace=True)

    # IMPROVEMENT: Fill NaN so Excel output is readable without manual filtering
    merged = merged.fillna('')

    log(f"[OK] Merged dataset: {len(merged)} rows, {len(merged.columns)} columns.")
    return merged


# === TARGET PACKAGE LOGIC ===
def build_target_package(merged):
    log("Building target_package tab...")

    target_cols = [
        'username_vdi', 'directoryid_vdi', 'workspaceid_vdi', 'ipaddress_vdi', 'computername_vdi',
        'asset id_qualys', 'agent host_qualys', 'ipv4_qualys', 'os_qualys', 'version_qualys',
        'last checked-in_qualys', 'short_host_qualys'
    ]
    # Add all RSA columns dynamically
    target_cols += [col for col in merged.columns if col.endswith('_rsa')]
    # Only keep target_cols that actually exist in the merged frame
    target_cols = [col for col in target_cols if col in merged.columns]

    target_sections = []

    # 1. Install Qualys: In WorkSpaces but not in Qualys
    install_qualys_df = merged[
        merged['computername_vdi'].notna() & (merged['computername_vdi'] != '') &
        (merged['short_host_qualys'].isna() | (merged['short_host_qualys'] == ''))
    ].copy()

    if not install_qualys_df.empty:
        install_qualys_df.insert(0, 'Action', 'Install Qualys')
        install_qualys_df.insert(1, 'Team', 'Team')
        install_qualys_df = install_qualys_df[['Action', 'Team'] + target_cols]
        install_qualys_df.reset_index(drop=True, inplace=True)
        target_sections.append(install_qualys_df)
        log(f"[OK] Install Qualys rows: {len(install_qualys_df)}")

    # 2. Clear Asset from Qualys: In Qualys but not in WorkSpaces
    clear_qualys_df = merged[
        merged['short_host_qualys'].notna() & (merged['short_host_qualys'] != '') &
        (merged['computername_vdi'].isna() | (merged['computername_vdi'] == ''))
    ].copy()

    if not clear_qualys_df.empty:
        clear_qualys_df.insert(0, 'Action', 'Clear Asset from Qualys')
        clear_qualys_df.insert(1, 'Team', 'Team')
        clear_qualys_df = clear_qualys_df[['Action', 'Team'] + target_cols]
        clear_qualys_df.reset_index(drop=True, inplace=True)
        target_sections.append(clear_qualys_df)
        log(f"[OK] Clear Asset from Qualys rows: {len(clear_qualys_df)}")

    # Deduplicate column names before concat
    def safe_dedup_columns(df):
        cols = pd.Series(df.columns)
        for dup in cols[cols.duplicated()].unique():
            dup_idx = cols[cols == dup].index.tolist()
            for i, idx in enumerate(dup_idx):
                cols[idx] = f"{dup}_{i}" if i > 0 else dup
        df.columns = cols
        return df

    deduped_sections = []
    for df in target_sections:
        if not df.empty:
            df = df.reset_index(drop=True)
            df = safe_dedup_columns(df)
            deduped_sections.append(df)

    if deduped_sections:
        target_package_df = pd.concat(deduped_sections, ignore_index=True)
    else:
        target_package_df = pd.DataFrame(columns=['Action', 'Team'] + target_cols)

    log(f"[OK] Target package total rows: {len(target_package_df)}")
    return target_package_df


# === EXCEL EXPORT WITH FORMATTING ===
# IMPROVEMENT: Auto-fit columns and freeze header row for readability
def export_to_excel(final_master, target_package_df, ws_df, rsa_df, qualys_df):
    log("Exporting results to Excel...")

    with pd.ExcelWriter(output_excel, engine='openpyxl') as writer:
        sheets = {
            'Master': final_master,
            'target_package': target_package_df,
            'Raw_Workspaces': ws_df,
            'Raw_RSA': rsa_df,
            'Raw_Qualys': qualys_df,
        }
        for sheet_name, df in sheets.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)

            # IMPROVEMENT: Freeze header row and auto-fit column widths
            worksheet = writer.sheets[sheet_name]
            worksheet.freeze_panes = 'A2'
            for col_cells in worksheet.columns:
                max_len = max(
                    (len(str(cell.value)) if cell.value is not None else 0)
                    for cell in col_cells
                )
                col_letter = col_cells[0].column_letter
                worksheet.column_dimensions[col_letter].width = min(max_len + 4, 50)

    log(f"[OK] Excel exported: {output_excel}")


# === ARCHIVE INPUT FILES ===
# IMPROVEMENT: Archive only runs after confirmed successful export
def archive_inputs():
    log("Archiving input files...")
    for file in [rsa_file, qualys_file, ws_file]:
        if os.path.exists(file):
            dest = os.path.join(archive_path, os.path.basename(file))
            shutil.move(file, dest)
            log(f"[OK] Archived: {os.path.basename(file)}")


# === MAIN ===
if __name__ == '__main__':
    try:
        preflight_checks()
        ws_df = pull_workspaces()
        rsa_df, qualys_df = load_sources()
        merged = merge_datasets(ws_df, rsa_df, qualys_df)
        final_master = merged.copy()
        target_package_df = build_target_package(merged)
        export_to_excel(final_master, target_package_df, ws_df, rsa_df, qualys_df)
        # IMPROVEMENT: Archive is gated — only runs if export completed without error
        archive_inputs()
        log("✅ Inventory report completed successfully: {output_excel}")
    except Exception as e:
        log(f"[FATAL] Script terminated with error: {e}")
        raise
