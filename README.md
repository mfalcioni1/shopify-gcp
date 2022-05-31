## Virtual Env Set-up

```sh
python -m venv env-shopify
./env-shopify/Scripts/Activate.ps1
./env-shopify/Scripts/python.exe -m pip install --upgrade pip
python -m pip install -r requirements.txt
```
### Updating Requirements
```sh
pip freeze > requirements.txt
```

## Local Config Format
`env_config.json` is expected in the `auth` directory that is provided in the `AUTH` environment variable.
```json
{
    "api-version": "2021-10",
    "apikey": "SHOPIFY_API_KEY",
    "password": "SHOPIFY_API_PASSWORD",
    "hostname": "SHOPIFY_HOSTNAME",
    "shared-secret": "SHOPIFY_API_SECRET",
    "project-id": "GCP_PROJECT_ID",
    "data-bot": "PATH_TO_DATABOT_JSON"
}
```

```sh
setx AUTH_PATH YOUR/AUTH/PATH/
setx AUTH %AUTH_PATH%/env_config.json
```

### Intuition for DB Updates
```py
old = pd.DataFrame({'A': [1, 2, 3, 5],
                   'B': [100, 200, 300, 50]}).set_index('A')
new = pd.DataFrame({'A': [3, 2, 1, 4],
                   'B': [30, 20, 10, 40]}).set_index('A')
old.update(new, join='left')
db_update = pd.concat([old, new], join="outer").drop_duplicates()
```