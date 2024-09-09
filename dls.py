import asyncio
import json
import os
from functools import cached_property, partial

import aiofiles
from aiofiles.os import remove
from aiofiles.tempfile import NamedTemporaryFile
from aiogoogle import Aiogoogle, HTTPError
from aiogoogle.auth.creds import ServiceAccountCreds
from aiogoogle.sessions.aiohttp_session import AiohttpSession

from connectors.logger import logger
from connectors.source import BaseDataSource, ConfigurableFieldValueError
from connectors.utils import (
    TIKA_SUPPORTED_FILETYPES,
    RetryStrategy,
    convert_to_b64,
    retryable,
)

RETRIES = 1
RETRY_INTERVAL = 2
FILE_SIZE_LIMIT = 10485760  # ~ 10 Megabytes

DRIVE_API_TIMEOUT = 1 * 60  # 1 min

ACCESS_CONTROL = "_allow_access_control"

FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"

DRIVE_ITEMS_FIELDS = "id,createdTime,driveId,modifiedTime,name,size,mimeType,fileExtension,webViewLink,permissions,owners,parents"

# Google Service Account JSON includes "universe_domain" key. That argument is not
# supported in aiogoogle library in version 5.3.0. The "universe_domain" key is allowed in
# service account JSON but will be dropped before being passed to aiogoogle.auth.creds.ServiceAccountCreds.
SERVICE_ACCOUNT_JSON_ALLOWED_KEYS = set(dict(ServiceAccountCreds()).keys()) | {
    "universe_domain"
}

# Export Google Workspace documents to TIKA compatible format, prefer 'text/plain' where possible to be
# mindful of the content extraction service resources
GOOGLE_MIME_TYPES_MAPPING = {
    "application/vnd.google-apps.document": "text/plain",
    "application/vnd.google-apps.presentation": "text/plain",
    "application/vnd.google-apps.spreadsheet": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
}

GOOGLE_DRIVE_EMULATOR_HOST = os.environ.get("GOOGLE_DRIVE_EMULATOR_HOST")
RUNNING_FTEST = (
    "RUNNING_FTEST" in os.environ
)  # Flag to check if a connector is run for ftest or not.

# jedrazb
creds = {
    "type": "service_account",
    "project_id": "decoded-nebula-388315",
    "private_key_id": "7ee6d5c3280ed1c52b0ab5d90633fae8e6305f64",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDOaT3qaPJuv7WB\nUtz6Ws4vJijKOcyiXgbgxyz5Koc+gc42503Z0iJSe6CHkTxybnjyeL0RW7SFehOR\nPKvQz27YzN9pWtIR7WeNaExMSTCpmlnL5Mimd+aQG9RtZp3NTGsG3cvVYq1XG4Q0\neKXRCdgome6APL6uKd9iYIc5A7HjGlB1fsuTOJg5syYs3tnUFVNoCtQn0KRyj8Oa\n1ArUzQvxtYSgTWTN4kazEHH0MD9RjycBVFfDUw/sapn64RgIfr+DBPiLg1vhkjUR\nhafiJANTmo7UZjE9+BCRP1Nm8rIOoptGYdBUxykveFTc26yCZgfzJuJTFDRjo9Wv\neLRizpaZAgMBAAECggEAFca6YKJDDm5VDbBibhq5pTWFSOV9anVZTCpTOzPSiiSL\nbxUqzFPMKW/PxTGsQwXW5nzYtI28XzccfJ5+IXTa9M4zdUPexQOrALsLzqILm18U\ngWtxBEgCx/oZqJOSiyPYaERq0me7UiTt5xPB6ap8Vrc7t2WBN/JktfYpWfli8Wqn\nAHFDQYvxvuPrFCEPbAmTrMyWtFP3N+m6ucR0h6qmLInuyzGpbJ6MUg2PExrcPk33\nVHRkZOsvhgka9uyLSm/nfdaHP0oZXY4EnHED9FZEsOF1BeZOYx/zxENZcWI1diFE\nz7gEsTqUulQ2CskLRgTemChVLnsDSkhY36mrN29scQKBgQD5JzlMU11Rwxxoo0Nb\nkXjTL1ht/vSa6o0fWtMRQJT7atHt6jSlJ1GyjbPGMZRp6zZmpZNofgpZB4LEiNJV\n3pvPfGAOthAB/Yav5HUPpRFA7yCFelYn5mzNr3XRcBWrfYNeqMcCOZ0SFPNNhGHj\nWXYlhFkmxGE8ZxAV8UxlZaje6QKBgQDUFVSCHLKpQii4RNTsN9ieN4L0hr2zxm2h\noD2cp2owdr3r2zr9BP690uasud5SylJzuj1RlKxgML5FwKTvkSheM2E0R9Z46s+l\n8fsn8yltYXVQj+PwBNuWeloHmYs6U1FTTbba9p+1XlM75PdkpGX/LEj+DGv4ozS9\ncTkSryEMMQKBgQDmrUcigIiE+7UguA/UBqKsPEiORqAnkgOoxQloDrgg8qx8rvug\n8P915jZMyHiVfD142dHZy+A+v3J2DgtXj2wItSmmVTAK97Ssw0MSggywZvVgP3zn\nIjMYWFwn1bNqfCGWEYN3buoa5KTC309WE+0MHTYBa45vQyikeCz4MCAiiQKBgDlX\nFwUiqy9WAtpnScYgB0InshFYyLttX8C+KUpMfUpjIFA/csBr9E/wUIvMqQ5rQ0Wk\nyeVftAdBxn/naPCYp3hondRbw+HnYoL0XLpdZQr8uZIxhSgkuBYEOIkre1gmpKtI\nLLMg0OdhfFGKQ1UDmcwW9hkx1JNh1OWPUiX6EY5RAoGAcNFqz801oHpZ/46uzV5D\nHx+PkuZdiDJ+JADALZVU3TjpocfdVqKK/eE2sU6v8i98QuoxKQVOeNSTebXkajbF\nwOFQI1fDltfqwjMhFQYv8ueUD3Aneso5/AYie9He6DTn7SHnug0G3GRwZJugB51B\nQGQA1FoGkuTmPQw43AXxLNY=\n-----END PRIVATE KEY-----\n",
    "client_email": "google-drive-connector@decoded-nebula-388315.iam.gserviceaccount.com",
    "client_id": "102321330546447468080",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/google-drive-connector%40decoded-nebula-388315.iam.gserviceaccount.com",
}

# # elastic ws
creds = {
    "type": "service_account",
    "project_id": "ent-search-dev",
    "private_key_id": "9a98c6fbe93256e2e606a898cba58ee3bf8a4ddc",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDGdEMrXXD47Cy4\njzoBJiq0lXPNMyqLCOSDudcmGKcLRpWvlMzYH1ZrzZQEeDOYoLpptyhqM2IlUr5l\nyK6QVFU8zzbXqeeogD9OJB+lPIY8goBi8Z3TZVGRgUEXesdxudpGDIQL1t+q7Nuf\ntxbm93eJXgTvC9sY2+GLAqD8RYgFhIlxQNciq04Qk0mMuX4AF9Xa+yQkAIwLnQM0\nsYhPYfYEjdwKukVyTdo3fEOIJmSqR2o4CTpkK3oDcTcBOSZXyGQVTAUZjfSym+jR\nLs4YTKifpvgevR7gQekjZqG1eNcPVeTe3mL4PsS94G7prUwbiuspIsJ0Z5iKrTbo\nejobxryVAgMBAAECggEAGi8m2abnjABlWiiTp6kog1EgyVdR6qxOsk6n43/nMmaq\nw1LnwDBKT8j9GaXecOnsDpy6+WA0N+Z19qoG1kY6RN62RrhFN4dMUvwTLNMShxNU\n6zkj8MtyerdKJlXt0MSL1yg5l/JO6pd71tPqcqj+HaLJFd1DmEESviunoAFwlAGP\nzAQWRBNkwA6jx5VmkWJBUozgb0uSCFANsGvQa43K/viqUE/aMWf7lg3hvS+OrfPT\nTe9cczNrRXBMh+gXqs5uY8Rp6pxw8ecxpnMpPKiOFNj7G9TeHQyPypyQ/w9ZrlJT\n3bhs1NSdizSMINtAK+6fMMaywiYNW78+/J6fD2OlYQKBgQD/SGZu/pI4Uy7ws60C\nPedkBl9Vn4Qg2+ZNOAgaknIOnl+IrcuI2URdvhRqiSI/wwNYWwLhWUvpetttzEbE\nTaXRBz+sMZjeLeRRvqq309vt/+yHqtt5qnFMp4zNa9CSOeO5GHUQ18hatbqFcjav\nLEXN7fne1rL2TYASgCzce/ZuHQKBgQDHAv2wOdavQubiF86xJNcpqF80U4SjrWqu\n/Gj7XExJbi88JyJaoATPCxTHw4AEpdKN2+FMvU91vqDM15yCdfS1oN5R44kZhyuZ\nJ7+xQhuPrw/pfHArJgtxQSHfE0Lvui3OVpUPxeK663UgdkSUIuiIoOron7lxx806\nUdBDAiwe2QKBgQDHQukJzFWv92YVZfouXMmHrt2tROTqjRq3vXu92f+DX6VsV1Qe\nzpBpp5viCIaeRIcQ6YFXCs+RQF+U1bWLNpyuizztGI2dJy8ni29QK+NoY7Jptzi3\nQA5N6h6inXxhFySkbu25puTxwRYFYcBDfYhDFHVMtkNcs+O7DSd3Np+FFQKBgQDB\n4TkTQRXUwsG6DGWSEIkJjX3KtX7oEp93gHIqR74OX6jm2l1Omsx0hKAnCWAjpwTc\nrRTuNRQ/aoqvZXKVaqMPe3x7nMdJNnWUDLRk1TmSnoNK/a8tajAFkuWTtOkmMWRE\nu/cWWbvMTG1cRbwD8YpG6TZRkKISpgtbQK87SNeA6QKBgDgo2iLO/bVl1VD1+Gz/\nLVnPcW698QVHk52Z5Zzy00N6K0EhJDAsr082wDltKQ8SJsaRNQJ7OM4qinwrDrbZ\n2ph9E54lWJ8YP+U6WtayqypT6qV6mUQyGClmyj94lRGKnYgb8569pKGK8lozyH8t\npG0C9Vmj+4mZb1XsKC+Uwowi\n-----END PRIVATE KEY-----\n",
    "client_email": "connectors-2-0-poc@ent-search-dev.iam.gserviceaccount.com",
    "client_id": "101908041048836216920",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/connectors-2-0-poc%40ent-search-dev.iam.gserviceaccount.com",
}


# elastic connector
creds = {
    #   "type": "service_account",
    #   "project_id": "ent-search-dev",
    #   "private_key_id": "95fc129712886909f3c1bbea92b37d85bcf900e3",
    #   "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCzFx6Bpd1Ska0t\nRjg2iqY24+LntolYyuvUCn8ixmLi/S32RZV7z7LrjkN45dj8vwbFwnETY1QCEGKh\nL7qIJDwmiIMjYEbSpNB0Gi8pe+vkwqTJyR4+WoYBkWZSSIwZqdyylr0+wcSCTxu5\nlwZzbuIaogLIf8l6uRMRam3QCvgv3gy4TWyigubD2hUCddkHuFdliRSgfRh4ojbV\nuc9lc11L9YnXsWe3/L/d74LQb99BEfrmhUJW/mqCCgx7CJ4uVGy0jlw0FM92RJa5\nSzf/ckn5AWt6yvToEiTmM9o5Kld4ZKUt7iS4EoJX6w0z7PDombBn1gPo4YYLYo6H\nkOtpPNafAgMBAAECggEAWLhKzhpvtrBhVMz1Gxv7tLSXW4gDqZ+5TY23pvAV2/Jl\n2xtkrvXZYrVS+qkPIkK2JbEqWFr8KpuYaNaet0QQIly9boCLBV5NwD6af5ga5/R/\nM1G1cFljCpLP2FKFSU9rfHONo/HVGmL7TqkhMn1LeLsq2s3nb6sQ5a9OYGKWygsG\nHdzQA+i8ZnnxJ6bXxvg7aaqSziuAIPOs3f9f8j74qK++RLs/1et1vsmPnhx/ybZD\nUUO1//9D4/2997it944+3qAKxPyDSYuX/hOQqh/SLNDYIix1Yf7r2MaRS+1drgkh\nst+2dN812FBG43zyMz/0EPzM0GhJ79BshTWtxymfzQKBgQD157y+q998H5US8j64\npAhhhYEe8vOef2pwIdcJ2YwnCfs0lqsyBoZ9fct8zmUSJo6dh7e3MQGU8tLc5d6q\nPsjG2zlOuQsMlf3ZAuC6vk/Osyf12euuAQelQ0tU4aS5KMGpDw4CIZjb6SVYm91j\nv1rztvAXMmIsXeBFaGM/Nl8AmwKBgQC6cTY4IaU3QtHIZC7qMGUf1hKHoddQuSZB\nzH09gTg2PZ9E2GzLMUOz9jHbyT7Iv9WsL2TQ7/UJatpEzTew6zgICuHxjjRXoInY\ns6XamqNVLdjiX/xBC1VgUsHmOb0j4HFy+NCpewqyp4McU99EDKQNtNh9dwqtKvYQ\nKb8q7rN4TQKBgQCpxxXJj2D9kdCbBCXJ1Xbz3fkIJTWOeOpEARk287yQN5P4+w23\nvEme2vtjcBzYK0bL/KgbEvrEm+J6ITtaz35PzYwDjj7SsbxR2GrQhBUV4Rv14j+W\nI5/julKIiEHylPEgiARj9E5VhuMCCpsQA04IdoXNfjvJ5gax5SCgxClTgQKBgDjF\niHEHLbvqCN8wQmjlW6WaFgJe8aWO+2tFzU0RMAC0Ou9Hx7kHw0nlScFXQlr2ryXD\nOLu2lbLMarJQmwiwV1t7lNltJ9sW82KdMX+jXuDtGQG1oKZdwR2XlZt5MXLfQSx1\nP3ScFuHXyECz+WRLRRk3XES4HvQkBNFfJOr5zIRxAoGBAOxqCDsGWOo8Ff/rk+TO\nKNlAy1TsxBJG+pytsMfLy4/IFTic+q91hOACReUUw66A2tK+0mIx/eL2a3x0YjTu\nfuQI0Kx4w26eF41MUG1G3dcPGAaxkK2KY9cMrc+ND55AwyfDzwTOQ92aWho0pA2W\nPvd9q1iLlKFSQQTvt/DpbY2k\n-----END PRIVATE KEY-----\n",
    #   "client_email": "connectors-2-0-poc@ent-search-dev.iam.gserviceaccount.com",
    #   "client_id": "101908041048836216920",
    #   "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    #   "token_uri": "https://oauth2.googleapis.com/token",
    #   "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    #   "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/connectors-2-0-poc%40ent-search-dev.iam.gserviceaccount.com"
}

etl_creds = {
    "type": "service_account",
    "project_id": "gdrive-connector-392614",
    "private_key_id": "4830cb473788c08936937f36781638e0686bf5c1",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDOt+yD2lzJuAWP\n+v3NpSklzEU21NOST7E5aOt9Hgqkne6XXT1/cqMEw7v92hxiPfLCMR3YMyCZjib8\nrzurbKuJfB5qnHHI/5mgPfp8PlTgWj83iTTzoseXE37yVIwJyLnAJTMaH7W3N74C\nyvy8fUETKkkNlZ+IMdTzWUfK1TYTsSZazhTuhqMG+s9z1AxEfmWPCJfWdSrCajyy\nGn2bB9LX4WFAkL6cWwK5ARUt89UppE7pp+mkXKMDKbT2rkGkp27tlbxYEbF5NJuH\nfEv0nbHpX7YNsS868Mja8imEbC5Bi8RRJQ7IsupOQurTDxHMsXiMRh9xoZ5t9T8r\ngvdJQTSPAgMBAAECggEAVZ3gtCWTK6rvhtgeGXfP6ItHmXOhYyZlkREPA6WXI+IT\niRaBm5EXyqfk6zlUay4cJujf4wUd+eth42MSdCgQjeNd16sPB4AxmShCYAYS8k7+\ngtptl4DyaHSXLfftfjnoERh3J1k90nIgXRjEf67i7nSjiCBR0D9PYzC1puESwils\nM8jxauZVDHIAt2c7A7DFuP8DDauAM3LUlXKCGk54CYjtC8C5p52Yoa59z9wVxD5u\nNV9D0rSAV/KSH9A8vFfb7MHTt3bKOnKK8dHM1/04qwIMR4N4nKeBmwLKPjZpX+vJ\nKNIeHc3U8fQmhtFC29tojDmLbtgEndCxnj3cps8ZPQKBgQD5147/IUx/zXa99Rvc\n/zUtZAxcBH6EmOdVYoeJax1L9d3uoOgBN+TEr9+kNU5EUKgW5nDYeWWk8sueGk0W\nKj8eOZRj+qdNPE4ku3yOHknnJ69fGh8c9wP/9tRv6VpIAbm3QKk3JIW1+YQPXdX6\nUJ16Kp630hqkIuzehUFcxpemBQKBgQDT0EQnETcjj8PhHiSUerFtYb3UpOZiy2Gj\njHPbs028njPOZzlccIlPaQqo8DdRGRQwZNWofoTTTq+faLvMy8fDQ8UONBK8FrcA\ndBnl2z/3QGbaP94uY3GDs9aGe6l76sRcVEdIMO49A75R4g2tc5jHVFJjmNYPXA7g\nWcEG4m5AgwKBgQCzhJF4/IItKoLxhtcYej/Jd6Px0kDeIcfG8F8W/GI2nFl1ByTt\nCy5TrVNyd+Le71135uiags/oL5Ti618MWBq2KwVWuVnRA03a0uRycY0EjN1T5j8I\n3ux+1Opb5z45kxPQeqObvaBd2wVqzS/dBmPAzjLxPgRqf7XXRcRrVz7LyQKBgAWC\nsJM3nnCdZ+Iub5MxUfZfiZnDpH7DfcnMeKMpmgcjMoPXKaXfA6N+PxnADiBc/6p2\npRxaFkiNDoKeLeyZDJxm4fSJpEcibCZl3tqdBp//G+3npXcdfTGI9zbOds1TgUJJ\nqwUUyM3rkAL/V43AqSPebLpMmp0Rrw1lKGFueKHTAoGBAM94yQBRaqf4kmMOZomr\nOvO/EUwYXJqG4tM87j27KWajF7LGCeH6jDAXpwosguwjVVd6U7PjlgeVmV6T8bsS\ntjCvJyBf7EdBlGkUbLAwfA3nZF0marXDMXNsC3HUDnI9WxmKcc3xaL4UiHfVREqm\nLivYrwF5nDmawoDJdDNt44fi\n-----END PRIVATE KEY-----\n",
    "client_email": "drive-connector@gdrive-connector-392614.iam.gserviceaccount.com",
    "client_id": "101004850326939503455",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/drive-connector%40gdrive-connector-392614.iam.gserviceaccount.com",
}

etl_creds = {
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "client_email": "drive-connector@gdrive-connector-392614.iam.gserviceaccount.com",
    "client_id": "101004850326939503455",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/drive-connector%40gdrive-connector-392614.iam.gserviceaccount.com",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDNTMqkrPbFNlJy\nBr5F6WzVdQzaqLwl4meRIBZRuRq5dQZGgaZ4cLUL24vbobUdd41YQ0GuScY9bgtq\npZHThZwNQKsCjfsQXXRzdQ6rsyDGYSp3ZsYJgqSCDVHbej0cm6TGc176enx5dn53\nbf35xkDvz881+mDhtNe+EtsWzUzAe9MaeiWCamaGhHbq8yxMPveC93TNdB9BSw9j\niphecrtakbH5GZ2lhcCNrT7tmo+WvL5GmtiDYnN7RpLIaR+4XACdB2t8vLrGergD\nKFvc3YDUJD0WPFTSMuVPkXOvrSdZMSK5hLPArzd5I7y4dvqbyjiThmE4vCgWIFSp\nNOYMx0/9AgMBAAECggEABw3FJajStr5x0Tylk3ncdS4XIbkr88JGbDBVy/uH7UjV\nzRzdxyQaOFzT/pkdA7TsnJR9AHvE7a3nAwRK0SF1Veg7p/QQZQTNAD2Moj4emWpT\nvvP/SGrec6B5CEk4KdA4NmBW6lTEi5jUag6TrB4Rc5vogvKGGyMvSXbU48Dsiu12\na6LazeRpowdndHFEMxonAHFzwZ3bSZJFYXT2uJWtcjTWPNLSqJ1GOEEPvYKYFugv\nNyK8pQXNgnb8BjZ33zRJEiaCyJMOlm1GtzL7YIwsq/TaeK2CL64ykL6wW8kEui/P\n5LgVLN79ZMxBQRWjBrKjMIJv9uXHQtHUIxQjeZJOOQKBgQD3p/bKq6tQHkxAejtb\nNf67TT+zPYA5Izyq27ZsNiWZy9lTiRzZrBuvWDo5XNdGmFRQEQSTrR7iDIXvZp49\n8Is/tcxjYJ2AnOfqGtcrvCLdjiqWryQnykZnhbbS4bB8tbwk79eCZylLp+y5O3uR\nzVwWDurlchD7XDWAKRD9ean/9QKBgQDUN4FmhVH4irg6jPHWyUzQgblwUs2uiBCd\ndomLKvTDNWKRdzpqZOpMm7WWZcdWlFyJOyKAUubhEe0BuMQFxaMwjbDg0gsHZuVa\n4pTwLJqNbhrMdOj0FFuJtDlgX8d1YVBRQrrgPNODMLm410rPD3Ehym03A0QRoOyD\nDXtNFKGy6QKBgBkeQQqnRLXYjNVRUGnc3Rw0JozQZQvmhgWySRtKlp+1rDIryvBM\n43XyVd1xjCgN1SdVwogT2/kDWiZA2zXfCtCIaNV/fBoKT/9Pf5lg+IxOGgW0d89N\nVby0NCnJWDQd3ppEdTA1tFuHCZxz6wGLuZZHJw3kdtJ3tLlt8No0l8htAoGAMofo\n6vVx2EGHroFjEGasQLWy5xkAWr+RXT0dcSLQVy7+Am8mXLEczRMCgQZJGceH5TFM\nfMvAp/Txv0g0fL9bpTMH2/CHqcKJeEOdfTbRsj12ahqoYX37ceWVB+qmfr+mNUMz\nZjN1lbZjcqzo5jbiFE60BLZlXeE4j9gE9QylmAkCgYEAoM2MV3MKqqY18th2Fift\nK6DEG7kKrIAeMl0aiXlFq23oJx1o5xio7CvHBL39jay5APor2EVZoO/fn4P1K3ar\n13j6tnFyC88v2UEUalIHcUdladoLdkhtiHSrbvT12nDQeeXn1aw5630DtHD8s+HB\niLzREtePF9D+IDO+VomMr44=\n-----END PRIVATE KEY-----\n",
    "private_key_id": "5f21c387a4fcd8298ce761d3dbe80c8c67dc581b",
    "project_id": "gdrive-connector-392614",
    "token_uri": "https://oauth2.googleapis.com/token",
    "type": "service_account",
}


class RetryableAiohttpSession(AiohttpSession):
    """A modified version of AiohttpSession from the aiogoogle library:
    (https://github.com/omarryhan/aiogoogle/blob/master/aiogoogle/sessions/aiohttp_session.py)

    The low-level send() method is wrapped with @retryable decorator that allows for retries
    with exponential backoff before failing the request.
    """

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def send(self, *args, **kwargs):
        return await super().send(*args, **kwargs)


class GoogleAPIClient:
    """A google client to handle api calls made to Google Drive."""

    def __init__(self, json_credentials, scopes, api_name, api_version):
        """Initialize the ServiceAccountCreds class using which api calls will be made.

        Args:
            retry_count (int): Maximum retries for the failed requests.
            json_credentials (dict): Service account credentials json.
        """
        self.service_account_credentials = ServiceAccountCreds(
            scopes=scopes,
            subject="admin@extract-transform.com",
            **json_credentials,
        )
        self.api_name = api_name
        self.api_version = api_version
        self._logger = logger

    def set_logger(self, logger_):
        self._logger = logger_

    async def api_call_paged(
        self,
        resource,
        method,
        **kwargs,
    ):
        """Make a paged GET call to Google Drive API.

        Args:
            resource (aiogoogle.resource.Resource): Resource name for which the API call will be made.
            method (aiogoogle.resource.Method): Method available for the resource.

        Raises:
            exception: An instance of an exception class.

        Yields:
            async generator: Paginated response returned by the resource method.
        """

        async def _call_api(google_client, method_object, kwargs):
            page_with_next_attached = await google_client.as_service_account(
                method_object(**kwargs),
                full_res=True,
                timeout=DRIVE_API_TIMEOUT,
            )
            async for page_items in page_with_next_attached:
                yield page_items

        async for item in self._execute_api_call(resource, method, _call_api, kwargs):
            yield item

    async def api_call(
        self,
        resource,
        method,
        **kwargs,
    ):
        """Make a non-paged GET call to Google Drive API.

        Args:
            resource (aiogoogle.resource.Resource): Resource name for which the API call will be made.
            method (aiogoogle.resource.Method): Method available for the resource.

        Raises:
            exception: An instance of an exception class.

        Yields:
            dict: Response returned by the resource method.
        """

        async def _call_api(google_client, method_object, kwargs):
            yield await google_client.as_service_account(
                method_object(**kwargs), timeout=DRIVE_API_TIMEOUT
            )

        return await anext(self._execute_api_call(resource, method, _call_api, kwargs))

    async def _execute_api_call(self, resource, method, call_api_func, kwargs):
        """Execute the API call with common try/except logic.

        Args:
            resource (aiogoogle.resource.Resource): Resource name for which the API call will be made.
            method (aiogoogle.resource.Method): Method available for the resource.
            call_api_func (function): Function to call the API with specific logic.
            kwargs: Additional arguments for the API call.

        Raises:
            exception: An instance of an exception class.

        Yields:
            async generator: Response returned by the resource method.
        """
        try:
            async with Aiogoogle(
                service_account_creds=self.service_account_credentials,
                session_factory=RetryableAiohttpSession,
            ) as google_client:
                drive_client = await google_client.discover(
                    api_name=self.api_name, api_version=self.api_version
                )
                if RUNNING_FTEST and GOOGLE_DRIVE_EMULATOR_HOST:
                    drive_client.discovery_document["rootUrl"] = (
                        GOOGLE_DRIVE_EMULATOR_HOST + "/"
                    )

                resource_object = getattr(drive_client, resource)
                method_object = getattr(resource_object, method)

                async for item in call_api_func(google_client, method_object, kwargs):
                    yield item

        except AttributeError as exception:
            self._logger.error(
                f"Error occurred while generating the resource/method object for an API call. Error: {exception}"
            )
            raise
        except HTTPError as exception:
            self._logger.warning(
                f"Response code: {exception.res.status_code} Exception: {exception}."
            )
            raise
        except Exception as exception:
            self._logger.warning(f"Exception: {exception}.")
            raise


client = GoogleAPIClient(
    json_credentials=etl_creds,
    scopes=[
        "https://www.googleapis.com/auth/admin.directory.group.readonly",
        "https://www.googleapis.com/auth/admin.directory.user.readonly",
    ],
    api_name="admin",
    api_version="directory_v1",
)


drive_client = GoogleAPIClient(
    api_name="drive",
    api_version="v3",
    scopes=[
        "https://www.googleapis.com/auth/drive.readonly",
        "https://www.googleapis.com/auth/drive.metadata.readonly",
    ],
    json_credentials=etl_creds,
)


async def list_files():
    async for file in drive_client.api_call_paged(
        resource="files",
        method="list",
        corpora="user",
        q="trashed=false",
        orderBy="modifiedTime desc",
        fields=f"files,incompleteSearch,nextPageToken",
        includeItemsFromAllDrives=False,
        supportsAllDrives=False,
        pageSize=100,
    ):
        yield file


async def list_users():
    """Get files from Google Drive. Files can have any type.

    Yields:
        dict: Documents from Google Drive.
    """
    async for user in client.api_call_paged(
        resource="users",
        method="list",
        domain="extract-transform.com",
    ):
        yield user


# async def list_user_ta(user_id):
#         """Get files from Google Drive. Files can have any type.

#         Yields:
#             dict: Documents from Google Drive.
#         """
#         async for group in client.api_call_paged(
#             resource="users",
#             method="targetAudiences",
#             username=user_id,
#         ):
#             yield group


async def list_user_groups(user_id):
    """Get files from Google Drive. Files can have any type.

    Yields:
        dict: Documents from Google Drive.
    """
    async for group in client.api_call_paged(
        resource="groups",
        method="list",
        userKey=user_id,
    ):
        yield group


async def access_controls():

    users = []

    async for user_page in list_users():
        for user in user_page.get("users", []):
            user_id = user.get("id")
            email = user.get("primaryEmail")
            domain = email.split("@")[-1]
            permissions = [f"user:{email}"]
            permissions.append(f"domain:{domain}")
            groups = []

            # async for xd in list_user_ta(user_id):
            #     print(xd)
            async for groups_page in list_user_groups(user_id):
                print(groups_page.get("groups"), [])
                for group in groups_page.get("groups", []):
                    groups.append(group.get("email"))
                    permissions.append(f"group:{group.get('email')}")

            users.append(
                {
                    "_id": user.get("primaryEmail"),
                    "identity": {
                        "name": user.get("name").get("fullName"),
                        "email": user.get("primaryEmail"),
                        "groups": groups,
                        "domain": domain,
                    },
                    "query": {
                        "template": {"params": {"access_control": [permissions]}}
                    },
                }
            )

    print(users)


async def list_files_wrapper():
    async for file in list_files():
        print(file)


asyncio.run(list_files_wrapper())
