
import boto3
import pandas as .pd
import os
import shutil
import time
from datetime import datetime
from botocore. exceptions import ClientError

# === SETUP ·===
base_path =r'C: \temp\matrix'
archive_path = os.path. join(base_path, 'archive')
log_file = os.path. join(base_path, 'inventory_log. txt')
os.makedirs (archive_path, exist_ok=True)

rsa_file = os.path. join(base_path, 'rsadump.csv')
qualys_file = os.path. join(base_path, 'qualysdump.csv')
ws_file = os.path. join(base_path, 'workspaces_dump.csv')
output_excel = os.path.join(base_path, 'workspace_inventory_master.xlsx')

v def log(msg) :
.... with open(log_file, 'a') as.f:
f.write(f"{datetime.now()}-{msg}\n")
print (msg)

# === FLATTEN .UTILITY .===
v def flatten_dict(d, parent_key='', sep='.'):
items = []
for.k,v in d.items():
new_key = f"{parent_key} {sep}{k}".if parent_key else.k
if isinstance(v, dict):
items.extend(flatten_dict(v, new_key, sep=sep).items())
else:
items. append( (new_key. lower(), v))
.return dict(items)

# ===. SAFE . TAG . FETCH . ===
v def safe_get_tags(client, ws_id, retries=3):
.for attempt in range(retries):
try:
... return .client. describe_tags (ResourceId=ws_id).get('TagList', [])
except ClientError as e:
log(f"[!].Retrying tag fetch for {ws_id}: {e}")
time.sleep(1)

except Exception as e:
log(f"[!] Unexpected error for {ws_id}: {e}")
break
return []

# === WORKSPACES API ===
log("Pulling WorkSpaces data from AWS ... ")
client = boto3.client('workspaces', region_name='ca-central-1')
workspaces = []
next_token = None

while True:
response = client.describe_workspaces(NextToken=next_token) if next_token else client.describe_workspaces()
workspaces. extend(response[ 'Workspaces ' ])
next_token = response.get('NextToken')
if not next token:
break

flattened_rows = []
all_columns = set()

for ws in workspaces:
flat = flatten_dict(ws)
ws_id = ws.get('WorkspaceId')
tags = safe_get_tags(client, ws_id)
for tag in tags:
flat[f'tags.{tag["Key"].lower()}'] = tag['Value']
flattened_rows. append (flat)
all_columns.update(flat.keys())

normalized_rows = [{col: row.get(col, None) for col in all_columns} for row in flattened_rows]
ws_df = pd.DataFrame(normalized_rows)

# Filter only EC2AMAZ / WSAMZN
if 'computername' in ws_df.columns:
ws_df = ws_df[ws_df['computername'].astype(str).str.startswith(('EC2AMAZ', 'WSAMZN'), na=False)]

ws_df.columns = [col.strip().lower() + '_vdi' for col in ws_df.columns]
ws_df.to_csv(ws_file, index=False)

# === LOAD OTHER SOURCES ===
log("Loading RSA and Qualys files ... ")
rsa_df = pd.read_csv(rsa_file, skiprows=3)
qualys_df = pd.read_csv(qualys_file, skiprows=3)
rsa_df.columns = [col.strip().lower() for col in rsa_df.columns]
qualys_df.columns = [col.strip().lower() for col in qualys_df.columns]

rsa_df.rename(columns={'user id': 'username'}, inplace=True)
qualys_df.rename(columns={qualys_df.columns[2]: 'agent host', qualys_df.columns[3]: 'ipv4'}, inplace=True)
qualys_df['short_host'] = qualys_df['agent host'].astype(str).str.split('.').str[e]
qualys_df = qualys_df[qualys_df['short_host'].str.startswith(('EC2AMAZ', 'WSAMZN'), na=False)]

# Rename columns
qualys_df.columns = [col + '_qualys' for col in qualys_df.columns]
rsa_df.columns = [col + '_rsa' for col in rsa_df.columns]

# === MERGE PHASE ===
log("Merging datasets ... ")
merged = pd.merge(
ws_df,
qualys_df,
left_on=['computername_vdi', 'ipaddress_vdi'],
right_on=['short_host_qualys', 'ipv4_qualys'],
how='outer'

merged = pd.merge(
merged,
rsa_df,
left_on='username_vdi'.
right_on='username_rsa'
how='left'

merged.reset_index(drop=True, inplace=True)
final_master = merged.copy()

# === TARGET PACKAGE LOGIC ===
log("Building target_package tab ... ")
target_sections = []


# Define the columns to retain
v target_cols = [
'username_vdi', 'directoryid_vdi', 'workspaceid_vdi', 'ipaddress_vdi', 'computername_vdi',
'asset id_qualys', 'agent host_qualys', 'ipv4_qualys', 'os_qualys', 'version_qualys',
'last checked-in_qualys', 'short_host_qualys'

# Add all *_ rsa columns dynamically
target_cols += [col for col in merged.columns if col.endswith('_rsa')]

# 1. Install Qualys: In WorkSpaces but not in Qualys
v install_qualys_df = merged[
merged['computername_vdi'].notna() &
merged['short_host_qualys'].isna()
].copy()

v if not install_qualys_df.empty:
install_qualys_df.insert(0, 'Action', 'Install Qualys')
install_qualys_df.insert(1, 'Team', 'Team')
install_qualys_df = install_qualys_df[['Action', 'Team'] + target_cols]
install_qualys_df.reset_index(drop=True, inplace=True)
target_sections.append(install_qualys_df)

# 2. Clear Asset from Qualys: In Qualys but not in WorkSpaces
v clear_qualys_df = merged[
merged[' short_host_qualys' ].notna() &
merged['computername_vdi'].isna()
].copy()

v if not clear_qualys_df.empty:
clear_qualys_df.insert(0, 'Action', 'Clear Asset from Qualys')
clear_qualys_df.insert(1, 'Team', 'Team')
clear_qualys_df = clear_qualys_df[['Action', 'Team'] + target_cols]
clear_qualys_df.reset_index(drop=True, inplace=True)
target_sections.append(clear_qualys_df)

# Ensure all target sections have unique column names
v def safe_dedup_columns(df):
cols = pd.Series(df.columns)
for dup in cols[cols.duplicated()].unique():
dup idx = cols[cols == dupl.index.tolist()
dup_idx = cols[cols == dup].index.tolist()
for i, idx in enumerate(dup_idx):
cols[idx] = f"{dup}_{i}" if i > 0 else dup
df.columns = cols
return df

deduped_sections = []
for df in target_sections:
if not df.empty:
df = df.reset_index(drop=True)
df = safe_dedup_columns (df)
deduped_sections.append(df)

if deduped_sections:
target_package_df = pd.concat(deduped_sections, ignore_index=True)
else:
target_package_df = pd.DataFrame(columns=['Action', 'Team'] + target_cols)

log(f"

# === EXPORT RESULTS ===
log("Exporting results to Excel ... ")
with pd.ExcelWriter(output_excel, engine='openpyxl') as writer:
final_master.to_excel(writer, sheet_name='Master', index=False)
target_package_df.to_excel(writer, sheet_name='target_package', index=False)
ws_df.to_excel(writer, sheet_name='Raw_Workspaces', index=False)
rsa_df.to_excel(writer, sheet_name='Raw_RSA', index=False)
qualys_df. to_excel(writer, sheet_name='Raw_Qualys', index=False)

# === ARCHIVE INPUT FILES ==
log("Archiving input files ... ")
for file in [rsa_file, qualys_file, ws_file]:
if os.path.exists(file):
shutil.move(file, os.path. join(archive_path, os.path.basename(file)))

Inventory report completed successfully: {output_excel}")
