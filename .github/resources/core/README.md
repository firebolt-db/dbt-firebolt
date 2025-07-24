# Running tests for firebolt core over https

In order to run the integration tests using https protocol, we need to bring up an nginx reverse proxy. On this proxy we will install the server certificate. In our case it will be a self signed cert (localhost.pem) and its private key (localhost-key.pem). 

The cert and its key will be stored in GitHub since we cannot have secrets stored in code repository, and it would be easier to rotate them (no code changes required):
- FIREBOLT_CORE_DEV_CERT is the certificate
- FIREBOLT_CORE_DEV_CERT_PRIVATE_KEY is the private key

This is the certificate information that we currently have. Note that it wille expire in 2 years (when it will expire we will generate a new certificate and key and set them in the Git repository secrets)

Certificate Information:
- Common Name: 
- Subject Alternative Names: localhost
- Organization: mkcert development certificate
- Organization Unit: george@Mac.localdomain (George's Blue Mac)
- Locality: 
- State: 
- Country: 
- Valid From: May 22, 2025
- Valid To: August 22, 2027
- Issuer: mkcert george@Mac.localdomain (George's Blue Mac), mkcert development CA
- Key Size: 2048 bit
- Serial Number: 5b19e46ed1a5a912da2cef0129924c41


The client will connect over https to the nginx server which will do the TLS handshake and the TLS termination. It will then call the firebolt-core over http on port 3473.
