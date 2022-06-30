from catalog_config import CatalogConfig


def read_vault_secrets(secret_nm):
    config = CatalogConfig()
    config.read()

    with open(f"{config['SECRETS']['VAULT_SECRET_FOLDER']}/{config['SECRETS']['SECRETS_FILE']}") as file:
        for line in file.readlines():

            if line.startswith(f"{secret_nm}="):
                return line.split(f"{secret_nm}=")[1].replace(f'\n', '')

    raise Exception("env not found")


def read_env(secret_category, secret_nm):
    config = CatalogConfig()
    config.read()

    try:
        return f"{config[f'{secret_category}'][f'{secret_nm}']}"
    except Exception as e:
        print(e)