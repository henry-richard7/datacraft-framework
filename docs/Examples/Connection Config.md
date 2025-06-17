# Usage for `Connection Config`

#### Description

The `connection_config` field in the configuration table expects a dictionary (JSON format) depending on the type of connection.

---

## 🔐 SFTP

Example:

```json
{
  "host": "your-sftp-host.com",
  "port": "22",
  "user": "sftp-username"
}
```

---

## ☁️ S3

Example:

```json
{
  "client_id": "your-aws-client-id",
  "client_secret": "your-aws-secret-key",
  "endpoint_url": "https://s3.custom-endpoint.com",
  "signature_version": "s3v4"
}
```

> `endpoint_url` and `signature_version` are optional.

---

## 🗄️ Database

Example:

```json
{
  "url": "jdbc:mysql://host:3306",
  "database": "your-database-name",
  "user": "db-username",
  "password": "db-password",
  "driver": "com.mysql.cj.jdbc.Driver"
}
```

---

## 🧪 Veeva / Salesforce

Example:

```json
{
  "domain": "your-salesforce-domain.com",
  "client_id": "your-client-id",
  "client_secret": "your-client-secret"
}
```

---

> 💡 All values should be strings. Optional fields can be omitted based on your setup.
