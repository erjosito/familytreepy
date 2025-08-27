# Test project

Using this techs:

- Streamlit
- Azure Cosmos DB with Gremlin API
- Entra External ID

# Notes

## Entra External ID

- Application created for internal audiences, otherwise it was not visible when adding the app to the External ID flow.
- Error `The client does not exist or is not enabled for consumers`, changed the app's manifiest from

  ```
  "signInAudience": "AzureADMyOrg"
  ```

  to

  ```
  "signInAudience": "AzureADandPersonalMicrosoftAccount",
  "accessTokenAcceptedVersion": 2,
  ```

- However, saving the app fails with the error message "the app was not found".
