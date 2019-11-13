import pandas as pd
from pandas.io.json import json_normalize
import json

#import file and normalize json
data = json.load(open('leads.json'))
df = json_normalize(data['leads'])

#create compound index datetime and import order
df['entryDate_index'] = df['entryDate']
df['entryDate_index'] = pd.to_datetime(df['entryDate_index'])
df.set_index(['entryDate_index', df.index], inplace=True)

#Get all duplicated ids --------------------------------------------------------------
df_dup_ids = pd.DataFrame()
df_dup_ids_matching = pd.DataFrame()
mask_unique_ids = df.duplicated('_id', keep='last')
df_dup_ids = df_dup_ids.append(df.loc[mask_unique_ids])

#find replaced values
for index, row in df_dup_ids.iterrows():
    #find matching id
    matching_rec = df[index:].loc[df['_id'] == row['_id']].iloc[1]
    df_dup_ids_matching = df_dup_ids_matching.append(matching_rec)

df_dup_ids = df_dup_ids.rename(columns={'_id': '_id', 'email': 'from_email','firstName': 'from_firstName', 'lastName': 'from_lastName','address': 'from_address', 'entryDate': 'from_entryDate',})
df_dup_ids_matching = df_dup_ids_matching.rename(columns={'_id': '_id', 'email': 'to_email','firstName': 'to_firstName', 'lastName': 'to_lastName','address': 'to_address', 'entryDate': 'to_entryDate',})
df_dup_ids_combined = df_dup_ids.reset_index(drop=True).merge(df_dup_ids_matching.reset_index(drop=True), left_index=True, right_index=True)
#Set flag to know this was a _id dup
df_dup_ids_combined['dup_id'] = True

#Get all duplicated emails --------------------------------------------------------------
df_dup_emails = pd.DataFrame()
df_dup_emails_matching = pd.DataFrame()
mask_unique_emails = df.duplicated('email', keep='last')
df_dup_emails = df_dup_emails.append(df.loc[mask_unique_emails])

#find replaced values
for index, row in df_dup_emails.iterrows():
    #find matching email
    matching_rec = df[index:].loc[df['email'] == row['email']].iloc[1]
    df_dup_emails_matching = df_dup_emails_matching.append(matching_rec)

df_dup_emails = df_dup_emails.rename(columns={'_id': 'from_id', 'email': 'email','firstName': 'from_firstName', 'lastName': 'from_lastName','address': 'from_address', 'entryDate': 'from_entryDate',})
df_dup_emails_matching = df_dup_emails_matching.rename(columns={'_id': 'to_id', 'email': 'email','firstName': 'to_firstName', 'lastName': 'to_lastName','address': 'to_address', 'entryDate': 'to_entryDate',})
df_dup_emails_combined = df_dup_emails.reset_index(drop=True).merge(df_dup_emails_matching.reset_index(drop=True), left_index=True, right_index=True)
#Set flag to know this was a email dup
df_dup_emails_combined['dup_email'] = True

#create final log
df_all_logs = pd.concat([df_dup_ids_combined, df_dup_emails_combined], sort=True)
df_all_logs = df_all_logs.drop(['_id_x', '_id_y'] ,axis=1)
df_all_logs.to_csv('leads_log.csv', index=False)

#create formatted dedupped list
dedupped_list = df.loc[~mask_unique_ids & ~mask_unique_emails]
formatted_dedupped_list = {'leads': dedupped_list.to_dict(orient='records')}
with open('leads_dedupped.json', 'w', encoding='utf-8') as f:
    json_string = json.dumps(formatted_dedupped_list, indent=0)
    f.write(json_string)