# dremio-lineage
Tool to export dremio graph lineage to a json file. This can then be uploaded to the Dremio UI and queried.

# Usage
Configure config.yaml
- Dremio Software
-- required fields DremioCloud: False,  URL (including http(s):// and port), Username, Password
- Dremio Cloud
-- required fields DremioCloud: True, URL (control plane base url https://docs.dremio.com/cloud/api/) Password (PAT), ProjectID
